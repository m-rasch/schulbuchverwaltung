"""
╔══════════════════════════════════════════════════════════════════╗
║        SCHULBUCHVERWALTUNG  –  Streamlit + Firebase              ║
║        E-Mail/Passwort-Login  |  Firestore Datenbank             ║
║        3 MODI: EINZELN / DOPPEL / FLEXIBEL                       ║
╚══════════════════════════════════════════════════════════════════╝

Datenmodell pro Buch:
  isbn            – Primärschlüssel (String)
  titel           – Buchtitel
  fach            – Schulfach
  klasse          – zugeordnete Klasse(n), z.B. "5/6" oder "5a,6b,7c"
  modus           – "einzeln" / "doppel" / "flexibel"
  
  MODUS 1 - EINZELJAHRGANG:
    umlauf_klassen – dict {klasse: anzahl}
    verfuegbar_next = sum(umlauf_klassen) + max(lager - 5, 0)
  
  MODUS 2 - DOPPELJAHRGANG:
    jahrgang1_klassen – dict {klasse: anzahl} (behalten Bücher)
    jahrgang2_klassen – dict {klasse: anzahl} (geben zurück)
    verfuegbar_next = sum(jahrgang2) + max(lager - 5, 0)
  
  MODUS 3 - FLEXIBLER UMLAUF:
    flex_klassen – dict {klasse: {"umlauf": 12, "zurueck": 5}}
    verfuegbar_next = sum(alle "zurueck") + max(lager - 5, 0)
  
  lager           – Anzahl im Lager
  bedarf_next     – erwartete Schülerzahl nächstes Jahr
  anschaffung     – Datum der Anschaffung (String YYYY-MM-DD)
  bestellbar      – True/False (im Schulbuchkatalog verfügbar)
  notizen         – Freitext
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import json
import re
from datetime import datetime, date

import firebase_admin
from firebase_admin import credentials, firestore
import plotly.express as px
from fpdf import FPDF

# ═══════════════════════════════════════════════════════════════════════════════
#  KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

APP_TITLE = "📚 Schulbuchverwaltung"
RESERVE   = 5   # Mindestbestand im Lager

ALLE_KLASSEN = [
    "5a","5b","5c",
    "6a","6b","6c",
    "7a","7b","7c",
    "8a","8b","8c",
    "9a","9b","9c",
    "10a","10b","10c","10g1","10g2",
    "11/1","11/2",
    "12/1","12/2",
]

ALLE_FAECHER = [
    "Mathematik","Deutsch","Englisch","Französisch","Latein","Spanisch",
    "Physik","Chemie","Biologie","Mensch-Natur-Technik","Geographie","Geschichte","Politik",
    "Ethik","Religion","Musik","Kunst","Sport","Informatik","Wirtschaft",
    "Sonstiges",
]

# ═══════════════════════════════════════════════════════════════════════════════
#  FIREBASE
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner="Verbinde mit Firebase …")
def init_firebase():
    """Firebase Admin SDK initialisieren. Credentials aus st.secrets."""
    try:
        if firebase_admin._apps:
            return firestore.client()
        key_dict = dict(st.secrets["firebase"])
        if "private_key" in key_dict:
            key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
        cred = credentials.Certificate(key_dict)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        st.error(f"🔥 Firebase-Fehler: {e}")
        st.stop()


def col_ref(db):
    return db.collection("schulbuecher")


def load_all(db) -> list[dict]:
    """Alle Bücher aus Firestore laden."""
    try:
        docs = col_ref(db).stream()
        result = []
        for d in docs:
            row = d.to_dict()
            row["_id"] = d.id
            
            # Migration: alte Bücher mit doppeljahrgang=True zu modus="doppel"
            if "modus" not in row:
                if row.get("doppeljahrgang", False):
                    row["modus"] = "doppel"
                else:
                    row["modus"] = "einzeln"
            
            # Felder sicherstellen
            if row["modus"] == "doppel":
                if "jahrgang1_klassen" not in row:
                    row["jahrgang1_klassen"] = {}
                if "jahrgang2_klassen" not in row:
                    row["jahrgang2_klassen"] = {}
            elif row["modus"] == "flexibel":
                if "flex_klassen" not in row:
                    row["flex_klassen"] = {}
            else:
                if "umlauf_klassen" not in row:
                    row["umlauf_klassen"] = {}
            
            result.append(row)
        return result
    except Exception as e:
        st.error(f"Ladefehler: {e}")
        return []


def save_book(db, data: dict):
    """Buch anlegen oder überschreiben (ISBN = Dokument-ID)."""
    isbn = str(data.get("isbn","")).strip()
    if not isbn:
        st.error("ISBN darf nicht leer sein!")
        return False
    payload = {k: v for k, v in data.items() if k != "_id"}
    try:
        col_ref(db).document(isbn).set(payload)
        return True
    except Exception as e:
        st.error(f"Speicherfehler: {e}")
        return False


def delete_book(db, isbn: str):
    """Buch unwiderruflich löschen."""
    try:
        col_ref(db).document(isbn).delete()
        return True
    except Exception as e:
        st.error(f"Löschfehler: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  BERECHNUNGEN - 3 MODI
# ═══════════════════════════════════════════════════════════════════════════════

def berechne_felder(buch: dict) -> dict:
    """Berechnete Felder zu einem Buch-Dict hinzufügen."""
    lager  = int(buch.get("lager", 0))
    bedarf = int(buch.get("bedarf_next", 0))
    modus  = buch.get("modus", "einzeln")
    
    if modus == "doppel":
        # DOPPELJAHRGANGS-LOGIK
        jg1 = buch.get("jahrgang1_klassen", {}) or {}
        jg2 = buch.get("jahrgang2_klassen", {}) or {}
        
        jg1_gesamt = sum(int(v) for v in jg1.values())
        jg2_gesamt = sum(int(v) for v in jg2.values())
        umlauf_gesamt = jg1_gesamt + jg2_gesamt
        gesamt = umlauf_gesamt + lager
        
        # NUR Jahrgang 2 kommt zurück
        reserve_verfuegbar = max(lager - RESERVE, 0)
        verfuegbar_next = jg2_gesamt + reserve_verfuegbar
        
        buch["jahrgang1_gesamt"] = jg1_gesamt
        buch["jahrgang2_gesamt"] = jg2_gesamt
        
    elif modus == "flexibel":
        # FLEXIBLER UMLAUF - NEUE LOGIK
        flex = buch.get("flex_klassen", {}) or {}
        
        umlauf_gesamt = 0
        zurueck_gesamt = 0
        
        for kl, data in flex.items():
            if isinstance(data, dict):
                umlauf_gesamt += int(data.get("umlauf", 0))
                zurueck_gesamt += int(data.get("zurueck", 0))
        
        gesamt = umlauf_gesamt + lager
        
        # NUR die zurückkommenden Bücher
        reserve_verfuegbar = max(lager - RESERVE, 0)
        verfuegbar_next = zurueck_gesamt + reserve_verfuegbar
        
        buch["zurueck_gesamt"] = zurueck_gesamt
        
    else:
        # EINZELJAHRGANGS-LOGIK (Standard)
        uk = buch.get("umlauf_klassen", {}) or {}
        umlauf_gesamt = sum(int(v) for v in uk.values())
        gesamt = umlauf_gesamt + lager
        
        # Alle kommen zurück
        reserve_verfuegbar = max(lager - RESERVE, 0)
        verfuegbar_next = umlauf_gesamt + reserve_verfuegbar
    
    differenz = verfuegbar_next - bedarf
    
    buch["umlauf_gesamt"]   = umlauf_gesamt
    buch["gesamt"]          = gesamt
    buch["verfuegbar_next"] = verfuegbar_next
    buch["differenz"]       = differenz
    buch["alarm"]           = differenz < 0
    
    return buch


def buecher_zu_df(buecher: list[dict]) -> pd.DataFrame:
    """Liste von Buch-Dicts → übersichtlicher DataFrame."""
    rows = []
    for b in buecher:
        b = berechne_felder(b)
        
        modus = b.get("modus", "einzeln")
        
        # Umlauf-Spalte formatieren
        if modus == "doppel":
            jg1 = b.get("jahrgang1_klassen", {}) or {}
            jg2 = b.get("jahrgang2_klassen", {}) or {}
            jg1_str = ", ".join(f"{k}: {v}" for k, v in sorted(jg1.items()))
            jg2_str = ", ".join(f"{k}: {v}" for k, v in sorted(jg2.items()))
            umlauf_str = f"J1: {jg1_str} | J2: {jg2_str}" if jg1_str and jg2_str else jg1_str or jg2_str
        elif modus == "flexibel":
            flex = b.get("flex_klassen", {}) or {}
            parts = []
            for k, data in sorted(flex.items()):
                if isinstance(data, dict):
                    u = data.get("umlauf", 0)
                    z = data.get("zurueck", 0)
                    parts.append(f"{k}: {u}({z}↩)")
            umlauf_str = ", ".join(parts)
        else:
            uk = b.get("umlauf_klassen", {}) or {}
            umlauf_str = ", ".join(f"{k}: {v}" for k, v in sorted(uk.items()))
        
        rows.append({
            "isbn":            b.get("isbn",""),
            "titel":           b.get("titel",""),
            "fach":            b.get("fach",""),
            "klasse":          b.get("klasse",""),
            "modus":           {"einzeln": "Einzeln", "doppel": "Doppel", "flexibel": "Flexibel"}.get(modus, modus),
            "umlauf_klassen":  umlauf_str,
            "umlauf_gesamt":   b.get("umlauf_gesamt", 0),
            "lager":           b.get("lager", 0),
            "gesamt":          b.get("gesamt", 0),
            "bedarf_next":     b.get("bedarf_next", 0),
            "verfuegbar_next": b.get("verfuegbar_next", 0),
            "differenz":       b.get("differenz", 0),
            "alarm":           b.get("alarm", False),
            "anschaffung":     b.get("anschaffung",""),
            "bestellbar":      b.get("bestellbar", False),
            "notizen":         b.get("notizen",""),
        })
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTHENTIFIZIERUNG
# ═══════════════════════════════════════════════════════════════════════════════

def check_login(email: str, password: str) -> bool:
    try:
        users = dict(st.secrets.get("users", {}))
        return users.get(email.strip(), None) == password
    except Exception:
        return False


def render_login_page():
    st.set_page_config(page_title=APP_TITLE, page_icon="📚", layout="centered")
    st.title(APP_TITLE)
    st.markdown("---")
    st.subheader("🔐 Bitte anmelden")

    with st.form("login_form"):
        email    = st.text_input("E-Mail-Adresse", placeholder="lehrer@schule.de")
        password = st.text_input("Passwort", type="password")
        submit   = st.form_submit_button("Einloggen", use_container_width=True)

    if submit:
        if check_login(email, password):
            st.session_state["logged_in"] = True
            st.session_state["user_email"] = email
            st.rerun()
        else:
            st.error("❌ E-Mail oder Passwort falsch.")

    st.markdown("---")
    st.caption("Nutzer werden in der `secrets.toml` unter `[users]` verwaltet.")


# ═══════════════════════════════════════════════════════════════════════════════
#  EXPORT-FUNKTIONEN
# ═══════════════════════════════════════════════════════════════════════════════

def export_txt(df: pd.DataFrame) -> bytes:
    """Tabellarischer TXT-Export."""
    lines = []
    ts    = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    lines.append("=" * 120)
    lines.append(f"  SCHULBUCHVERWALTUNG - Export vom {ts}")
    lines.append("=" * 120)

    header = (
        f"{'ISBN':<18} {'Titel':<28} {'Fach':<14} {'Modus':<10} "
        f"{'Umlf':>5} {'Lag':>5} {'Ges':>5} "
        f"{'Bed.':>5} {'Verf':>5} {'Diff':>5} {'Anschaffung':<12} "
        f"{'Bestell.':<10}"
    )
    lines.append(header)
    lines.append("-" * 120)

    for _, r in df.iterrows():
        diff_str = str(int(r['differenz']))
        alarm    = " ⚠" if r['alarm'] else ""
        line = (
            f"{str(r['isbn']):<18} {str(r['titel'])[:27]:<28} "
            f"{str(r['fach'])[:13]:<14} {str(r.get('modus', 'Einzeln'))[:9]:<10} "
            f"{int(r['umlauf_gesamt']):>5} {int(r['lager']):>5} "
            f"{int(r['gesamt']):>5} {int(r['bedarf_next']):>5} "
            f"{int(r['verfuegbar_next']):>5} "
            f"{diff_str:>5}{alarm:<2} "
            f"{str(r['anschaffung']):<12} "
            f"{'Ja' if r['bestellbar'] else 'Nein':<10}"
        )
        lines.append(line)

    lines.append("=" * 120)
    lines.append(f"  Gesamt: {len(df)} Bücher  |  "
                 f"Alarm (Nachbestellen): {df['alarm'].sum()} Bücher")
    lines.append("=" * 120)
    return "\n".join(lines).encode("utf-8")


def export_pdf(df: pd.DataFrame) -> bytes:
    """PDF-Export mit fpdf2."""
    
    def clean_text(text):
        """Entferne alle Unicode-Zeichen die PDF nicht mag."""
        text = str(text)
        # Ersetze Gedankenstriche und andere problematische Zeichen
        replacements = {
            '\u2013': '-',  # en dash
            '\u2014': '-',  # em dash
            '\u2018': "'",  # left single quote
            '\u2019': "'",  # right single quote
            '\u201c': '"',  # left double quote
            '\u201d': '"',  # right double quote
            'ä': 'ae', 'ö': 'oe', 'ü': 'ue',
            'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
            'ß': 'ss',
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        # Entferne alle restlichen Non-ASCII Zeichen
        return text.encode('ascii', 'ignore').decode('ascii')
    
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Schulbuchverwaltung - Bestandsliste", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 6, f"Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')}", 
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    cols   = ["isbn","titel","fach","modus","umlauf_gesamt","lager",
              "gesamt","bedarf_next","verfuegbar_next","differenz","anschaffung","bestellbar"]
    labels = ["ISBN","Titel","Fach","Modus","Umlauf","Lager",
              "Ges.","Bedarf","Verf.","Diff","Anschaffung","Best."]
    widths = [25, 50, 22, 15, 12, 10, 10, 12, 12, 10, 22, 10]

    # Header
    pdf.set_fill_color(50, 80, 130)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 7)
    for lbl, w in zip(labels, widths):
        pdf.cell(w, 7, lbl, border=1, align="C", fill=True)
    pdf.ln()

    # Daten
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)
    for i, (_, row) in enumerate(df.iterrows()):
        fill = i % 2 == 0
        if fill:
            pdf.set_fill_color(235, 240, 250)
        else:
            pdf.set_fill_color(255, 255, 255)

        if row["alarm"]:
            pdf.set_text_color(180, 0, 0)
        else:
            pdf.set_text_color(0, 0, 0)

        values = [
            clean_text(row["isbn"])[:20],
            clean_text(row["titel"])[:35],
            clean_text(row["fach"])[:14],
            clean_text(row.get("modus", "Einzeln"))[:10],
            str(int(row["umlauf_gesamt"])),
            str(int(row["lager"])),
            str(int(row["gesamt"])),
            str(int(row["bedarf_next"])),
            str(int(row["verfuegbar_next"])),
            str(int(row["differenz"])),
            clean_text(row["anschaffung"])[:12],
            "Ja" if row["bestellbar"] else "Nein",
        ]
        for val, w in zip(values, widths):
            pdf.cell(w, 6, val, border=1, fill=fill)
        pdf.ln()

    return bytes(pdf.output())


# ═══════════════════════════════════════════════════════════════════════════════
#  FORMULAR MIT 3 MODI: EINZELN / DOPPEL / FLEXIBEL
# ═══════════════════════════════════════════════════════════════════════════════

def buch_formular(db, existing: dict | None = None):
    """
    Formular zum Anlegen oder Bearbeiten eines Buches.
    3 MODI: Einzeljahrgang / Doppeljahrgang / Flexibler Umlauf
    """
    is_new = existing is None
    if is_new:
        existing = {
            "isbn":"", "titel":"", "fach": ALLE_FAECHER[0],
            "klasse":"", "modus": "einzeln",
            "umlauf_klassen":{}, "jahrgang1_klassen":{}, "jahrgang2_klassen":{}, "flex_klassen":{},
            "lager":0, "bedarf_next":0,
            "anschaffung": str(date.today()),
            "bestellbar": True, "notizen":"",
        }
    
    # Eindeutige Form-ID basierend auf Kontext (neu vs. edit vs. detail)
    import hashlib
    form_context = f"{'new' if is_new else existing.get('isbn', 'edit')}"
    form_id = hashlib.md5(form_context.encode()).hexdigest()[:8]

    with st.form(key=f"buch_form_{form_id}"):
        st.markdown("### " + ("➕ Neues Buch anlegen" if is_new else f"✏️ Bearbeiten: {existing.get('titel','')}"))

        c1, c2 = st.columns(2)
        with c1:
            isbn   = st.text_input("ISBN *", value=existing["isbn"],
                                   disabled=not is_new,
                                   help="ISBN ist der eindeutige Schlüssel")
            titel  = st.text_input("Titel *", value=existing["titel"])
            fach_idx = ALLE_FAECHER.index(existing["fach"]) if existing["fach"] in ALLE_FAECHER else 0
            fach   = st.selectbox("Fach", ALLE_FAECHER, index=fach_idx)
            klasse = st.text_input("Klasse(n) / Stufe",
                                   value=existing["klasse"],
                                   help='z.B. "5/6" oder "5a,6b,7c"')
        with c2:
            lager   = st.number_input("Lager (Exemplare)", min_value=0,
                                      value=int(existing.get("lager", 0)))
            bedarf  = st.number_input("Bedarf nächstes Jahr (neue Schüler)", min_value=0,
                                      value=int(existing.get("bedarf_next", 0)),
                                      help="Anzahl Schüler die nächstes Jahr das Buch NEU bekommen")
            anschaffung = st.text_input("Anschaffungsdatum (JJJJ-MM-TT)",
                                        value=existing.get("anschaffung",""),
                                        placeholder="2023-08-01")
            bestellbar  = st.checkbox("Im Schulbuchkatalog bestellbar",
                                      value=existing.get("bestellbar", True))

        st.markdown("---")
        st.markdown("#### 📖 Umlauf-Modus wählen")
        
        # Radio Buttons für Modus-Auswahl
        modus_idx = {"einzeln": 0, "doppel": 1, "flexibel": 2}.get(existing.get("modus", "einzeln"), 0)
        modus_wahl = st.radio(
            "Wie wird dieses Buch genutzt?",
            options=["⚪ Einzeljahrgang", "🔵 Doppeljahrgang", "🟢 Flexibler Umlauf"],
            index=modus_idx,
            horizontal=True,
            help="Einzeln: Bücher werden jedes Jahr zurückgegeben | Doppel: Bücher werden 2 Jahre behalten | Flexibel: Jahrgangübergreifend mit individueller Rückgabe"
        )
        
        # Modus-String extrahieren
        if "Einzeljahrgang" in modus_wahl:
            modus = "einzeln"
        elif "Doppeljahrgang" in modus_wahl:
            modus = "doppel"
        else:
            modus = "flexibel"

        # MODUS-SPEZIFISCHE EINGABEN
        jg1_new = {}
        jg2_new = {}
        uk_new = {}
        flex_new = {}
        
        if modus == "einzeln":
            # ═══ MODUS 1: EINZELJAHRGANG ═══
            st.info("📕 **Einzeljahrgangs-Buch**: Schüler geben das Buch am Ende des Schuljahres zurück.")
            st.caption("Trage ein, wie viele Exemplare aktuell in welcher Klasse im Umlauf sind.")

            klassen_str = st.text_input(
                "Klassen im Umlauf",
                value=", ".join(existing.get("umlauf_klassen", {}).keys()),
                placeholder="z.B. 7a, 7b, 7c",
                key=f"umlauf_klassen_str_{form_id}"
            )
            
            if klassen_str.strip():
                klassen_list = [k.strip() for k in re.split(r"[,;]+", klassen_str) if k.strip()]
                uk_existing = existing.get("umlauf_klassen", {}) or {}
                
                n_cols = min(len(klassen_list), 4)
                cols = st.columns(n_cols)
                
                for i, kl in enumerate(klassen_list):
                    with cols[i % n_cols]:
                        val = st.number_input(
                            f"Klasse {kl}",
                            min_value=0,
                            value=int(uk_existing.get(kl, 0)),
                            key=f"uk_{form_id}_{kl}"
                        )
                        if val > 0:
                            uk_new[kl] = val
            
            # Berechnung anzeigen
            umlauf_summe = sum(uk_new.values())
            verf_next = umlauf_summe + max(lager - RESERVE, 0)
            diff = verf_next - bedarf
            
            st.markdown("---")
            col_calc1, col_calc2, col_calc3 = st.columns(3)
            col_calc1.metric("Im Umlauf", umlauf_summe)
            col_calc2.metric("Verfügbar nächstes Jahr", verf_next,
                           help=f"Alle kommen zurück ({umlauf_summe}) + Lager über Reserve ({max(lager-RESERVE, 0)})")
            col_calc3.metric("Differenz", diff, delta_color="inverse" if diff < 0 else "normal")
            
            if diff < 0:
                st.error(f"⚠️ Es fehlen {abs(diff)} Bücher für nächstes Jahr!")
        
        elif modus == "doppel":
            # ═══ MODUS 2: DOPPELJAHRGANG ═══
            st.info(
                "📘 **Doppeljahrgangs-Buch**: Schüler behalten das Buch 2 Jahre.\n\n"
                "**Beispiel Schuljahr 25/26:**\n"
                "- Jahrgang 1: 5a,5b,5c (bekommen 25/26, behalten bis 26/27)\n"
                "- Jahrgang 2: 6a,6b,6c (haben seit 24/25, geben 26/27 ab)"
            )
            
            col_jg1, col_jg2 = st.columns(2)
            
            with col_jg1:
                st.markdown("**🟢 Jahrgang 1** (behalten Bücher)")
                jg1_klassen_str = st.text_input(
                    "Klassen Jahrgang 1",
                    value=", ".join(existing.get("jahrgang1_klassen", {}).keys()),
                    placeholder="z.B. 5a, 5b, 5c",
                    key=f"jg1_klassen_{form_id}"
                )
                
                if jg1_klassen_str.strip():
                    jg1_klassen_list = [k.strip() for k in re.split(r"[,;]+", jg1_klassen_str) if k.strip()]
                    jg1_existing = existing.get("jahrgang1_klassen", {}) or {}
                    
                    for kl in jg1_klassen_list:
                        val = st.number_input(
                            f"Klasse {kl}",
                            min_value=0,
                            value=int(jg1_existing.get(kl, 0)),
                            key=f"jg1_{form_id}_{kl}"
                        )
                        if val > 0:
                            jg1_new[kl] = val
            
            with col_jg2:
                st.markdown("**🔴 Jahrgang 2** (geben zurück)")
                jg2_klassen_str = st.text_input(
                    "Klassen Jahrgang 2",
                    value=", ".join(existing.get("jahrgang2_klassen", {}).keys()),
                    placeholder="z.B. 6a, 6b, 6c",
                    key=f"jg2_klassen_{form_id}"
                )
                
                if jg2_klassen_str.strip():
                    jg2_klassen_list = [k.strip() for k in re.split(r"[,;]+", jg2_klassen_str) if k.strip()]
                    jg2_existing = existing.get("jahrgang2_klassen", {}) or {}
                    
                    for kl in jg2_klassen_list:
                        val = st.number_input(
                            f"Klasse {kl}",
                            min_value=0,
                            value=int(jg2_existing.get(kl, 0)),
                            key=f"jg2_{form_id}_{kl}"
                        )
                        if val > 0:
                            jg2_new[kl] = val
            
            # Berechnung anzeigen
            jg1_summe = sum(jg1_new.values())
            jg2_summe = sum(jg2_new.values())
            verf_next = jg2_summe + max(lager - RESERVE, 0)
            diff = verf_next - bedarf
            
            st.markdown("---")
            col_calc1, col_calc2, col_calc3, col_calc4 = st.columns(4)
            col_calc1.metric("Jahrgang 1 (behalten)", jg1_summe)
            col_calc2.metric("Jahrgang 2 (zurück)", jg2_summe)
            col_calc3.metric("Verfügbar nächstes Jahr", verf_next,
                           help=f"Jahrgang 2 ({jg2_summe}) + Lager über Reserve ({max(lager-RESERVE, 0)})")
            col_calc4.metric("Differenz", diff, delta_color="inverse" if diff < 0 else "normal")
            
            if diff < 0:
                st.error(f"⚠️ Es fehlen {abs(diff)} Bücher für nächstes Jahr!")
        
        else:  # modus == "flexibel"
            # ═══ MODUS 3: FLEXIBLER UMLAUF ═══
            st.info(
                "🟢 **Flexibler Umlauf**: Jahrgangübergreifende Nutzung mit individueller Rückgabe.\n\n"
                "**Beispiel DaZ Band 1:**\n"
                "- 5a: 8 Schüler haben es, 3 geben ab (erreichen Sprachniveau)\n"
                "- 6b: 5 Schüler haben es, 2 geben ab\n"
                "- 7c: 4 Schüler haben es, 4 geben ab"
            )
            
            flex_klassen_str = st.text_input(
                "Klassen im Umlauf",
                value=", ".join(existing.get("flex_klassen", {}).keys()),
                placeholder="z.B. 5a, 6b, 7c, 8a",
                key=f"flex_klassen_{form_id}"
            )
            
            if flex_klassen_str.strip():
                flex_klassen_list = [k.strip() for k in re.split(r"[,;]+", flex_klassen_str) if k.strip()]
                flex_existing = existing.get("flex_klassen", {}) or {}
                
                st.markdown("**Pro Klasse: Wie viele haben das Buch und wie viele geben es zurück?**")
                
                n_cols = min(len(flex_klassen_list), 3)
                cols = st.columns(n_cols)
                
                for i, kl in enumerate(flex_klassen_list):
                    with cols[i % n_cols]:
                        st.markdown(f"**Klasse {kl}**")
                        
                        existing_data = flex_existing.get(kl, {}) if isinstance(flex_existing.get(kl), dict) else {}
                        
                        umlauf = st.number_input(
                            f"Im Umlauf",
                            min_value=0,
                            value=int(existing_data.get("umlauf", 0)),
                            key=f"flex_umlauf_{form_id}_{kl}"
                        )
                        
                        zurueck = st.number_input(
                            f"Davon zurück ↩",
                            min_value=0,
                            value=int(existing_data.get("zurueck", 0)),
                            key=f"flex_zurueck_{form_id}_{kl}",
                            help="Wie viele Schüler geben das Buch nächstes Jahr ab?"
                        )
                        
                        if umlauf > 0:
                            flex_new[kl] = {"umlauf": umlauf, "zurueck": zurueck}
            
            # Berechnung anzeigen
            umlauf_summe = sum(d.get("umlauf", 0) for d in flex_new.values())
            zurueck_summe = sum(d.get("zurueck", 0) for d in flex_new.values())
            behalten_summe = umlauf_summe - zurueck_summe
            verf_next = zurueck_summe + max(lager - RESERVE, 0)
            diff = verf_next - bedarf
            
            st.markdown("---")
            col_calc1, col_calc2, col_calc3, col_calc4, col_calc5 = st.columns(5)
            col_calc1.metric("Im Umlauf gesamt", umlauf_summe)
            col_calc2.metric("Behalten", behalten_summe, help="Schüler die das Buch nächstes Jahr weiter nutzen")
            col_calc3.metric("Kommen zurück ↩", zurueck_summe)
            col_calc4.metric("Verfügbar nächstes Jahr", verf_next,
                           help=f"Zurück ({zurueck_summe}) + Lager über Reserve ({max(lager-RESERVE, 0)})")
            col_calc5.metric("Differenz", diff, delta_color="inverse" if diff < 0 else "normal")
            
            if diff < 0:
                st.error(f"⚠️ Es fehlen {abs(diff)} Bücher für nächstes Jahr!")

        notizen = st.text_area("Notizen", value=existing.get("notizen",""),
                               placeholder='z.B. "DaZ Band 1 für Anfänger"')

        submitted = st.form_submit_button("💾 Speichern", use_container_width=True)

    if submitted:
        if not isbn.strip() or not titel.strip():
            st.error("ISBN und Titel sind Pflichtfelder.")
            return False

        buch = {
            "isbn":           isbn.strip(),
            "titel":          titel.strip(),
            "fach":           fach,
            "klasse":         klasse.strip(),
            "modus":          modus,
            "lager":          lager,
            "bedarf_next":    bedarf,
            "anschaffung":    anschaffung.strip(),
            "bestellbar":     bestellbar,
            "notizen":        notizen.strip(),
        }
        
        # Modus-spezifische Daten
        if modus == "doppel":
            buch["jahrgang1_klassen"] = jg1_new
            buch["jahrgang2_klassen"] = jg2_new
            buch["umlauf_klassen"] = {}
            buch["flex_klassen"] = {}
        elif modus == "flexibel":
            # Validierung: zurück darf nicht größer als umlauf sein
            fehler = []
            for kl, data in flex_new.items():
                if data.get("zurueck", 0) > data.get("umlauf", 0):
                    fehler.append(f"Klasse {kl}: Es können nicht mehr Bücher zurückkommen ({data['zurueck']}) als im Umlauf sind ({data['umlauf']})")
            
            if fehler:
                st.error("❌ Fehler:\n\n" + "\n".join(fehler))
                return False
            
            buch["flex_klassen"] = flex_new
            buch["umlauf_klassen"] = {}
            buch["jahrgang1_klassen"] = {}
            buch["jahrgang2_klassen"] = {}
        else:
            buch["umlauf_klassen"] = uk_new
            buch["jahrgang1_klassen"] = {}
            buch["jahrgang2_klassen"] = {}
            buch["flex_klassen"] = {}
        
        # Legacy-Feld für Kompatibilität
        buch["doppeljahrgang"] = (modus == "doppel")
        
        if save_book(db, buch):
            st.success(f"✅ Buch '{titel}' erfolgreich gespeichert!")
            st.session_state.pop("edit_isbn", None)
            st.session_state["reload"] = True
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  HAUPTANWENDUNG
# ═══════════════════════════════════════════════════════════════════════════════

def main_app():
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📚",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    db = init_firebase()

    if st.session_state.get("reload") or "buecher" not in st.session_state:
        st.session_state["buecher"] = load_all(db)
        st.session_state.pop("reload", None)

    buecher = st.session_state["buecher"]
    df      = buecher_zu_df(buecher)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title(APP_TITLE)
        st.caption(f"Angemeldet als: **{st.session_state.get('user_email','')}**")

        if st.button("🚪 Abmelden", use_container_width=True):
            for k in ["logged_in","user_email","buecher","edit_isbn"]:
                st.session_state.pop(k, None)
            st.rerun()

        st.markdown("---")
        st.subheader("🔍 Filter")
        f_fach   = st.selectbox("Fach", ["Alle"] + sorted(df["fach"].unique().tolist()) if not df.empty else ["Alle"])
        f_klasse = st.selectbox("Klasse", ["Alle"] + ALLE_KLASSEN)
        f_alarm  = st.checkbox("Nur Alarm-Bücher (Nachbestellen)")
        f_text   = st.text_input("Suche (Titel / ISBN)", placeholder="Suche …")

        st.markdown("---")
        st.subheader("📥 Export")

        fname_txt = f"schulbuch_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        st.download_button(
            "⬇️ Backup als TXT",
            data    = export_txt(df) if not df.empty else b"Keine Daten",
            file_name = fname_txt,
            mime    = "text/plain",
            use_container_width=True,
        )

        if not df.empty:
            # PDF nur bei Klick generieren
            if st.button("📄 PDF generieren", use_container_width=True, key="gen_pdf"):
                with st.spinner("PDF wird erstellt..."):
                    pdf_bytes = export_pdf(df)
                    st.download_button(
                        "⬇️ PDF herunterladen",
                        data      = pdf_bytes,
                        file_name = f"schulbuecher_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime      = "application/pdf",
                        use_container_width=True,
                        key="download_pdf"
                    )

        st.markdown("---")
        st.subheader("📊 Bestand pro Klasse")
        if not df.empty:
            df_plot = (
                df.groupby("klasse")["gesamt"]
                .sum()
                .reset_index()
                .rename(columns={"klasse":"Klasse","gesamt":"Bücher gesamt"})
            )
            fig = px.bar(
                df_plot, x="Klasse", y="Bücher gesamt",
                color="Bücher gesamt",
                color_continuous_scale="Blues",
                title="Gesamtbestand nach Klasse",
                height=350,
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=10,r=10,t=40,b=30),
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(tickangle=-45),
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        if st.button("🔄 Daten neu laden", use_container_width=True):
            st.session_state["reload"] = True
            st.rerun()

    # ── Kopfzeile ─────────────────────────────────────────────────────────────
    st.title(APP_TITLE)

    # ── Alarm-Banner ─────────────────────────────────────────────────────────
    alarm_df = df[df["alarm"] == True] if not df.empty else pd.DataFrame()
    if not alarm_df.empty:
        with st.expander(
            f"⚠️ NACHBESTELL-ALARM: {len(alarm_df)} Buch/Bücher müssen nachbestellt werden!",
            expanded=True
        ):
            st.error("Die folgenden Bücher reichen für das nächste Schuljahr **nicht** aus "
                     f"(verfügbar < Bedarf, Mindestlagerbestand {RESERVE} Exemplare eingerechnet):")
            alarm_show = alarm_df[[
                "isbn","titel","fach","klasse","modus",
                "umlauf_gesamt","lager","verfuegbar_next","bedarf_next","differenz",
                "bestellbar","notizen"
            ]].copy()
            alarm_show.columns = [
                "ISBN","Titel","Fach","Klasse","Modus",
                "Umlauf","Lager","Verfügbar nächstes Jahr","Bedarf","Fehlend",
                "Bestellbar","Notizen"
            ]
            st.dataframe(
                alarm_show,
                use_container_width=True,
                hide_index=True,
            )

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_liste, tab_neu, tab_edit, tab_details = st.tabs([
        "📋 Bestandsliste",
        "➕ Buch hinzufügen",
        "✏️ Buch bearbeiten / löschen",
        "🔎 Buch-Details",
    ])

    with tab_liste:
        st.subheader("Aktueller Buchbestand")

        df_view = df.copy() if not df.empty else df
        if not df_view.empty:
            if f_fach != "Alle":
                df_view = df_view[df_view["fach"] == f_fach]
            if f_klasse != "Alle":
                df_view = df_view[
                    df_view["klasse"].str.contains(f_klasse, na=False) |
                    df_view["umlauf_klassen"].str.contains(f_klasse, na=False)
                ]
            if f_alarm:
                df_view = df_view[df_view["alarm"] == True]
            if f_text:
                mask = (
                    df_view["titel"].str.contains(f_text, case=False, na=False) |
                    df_view["isbn"].str.contains(f_text, case=False, na=False)
                )
                df_view = df_view[mask]

        if df_view.empty:
            st.info("Keine Bücher gefunden. Füge über den Tab '➕ Buch hinzufügen' dein erstes Buch hinzu.")
        else:
            show_cols = [
                "isbn","titel","fach","klasse","modus",
                "umlauf_klassen","umlauf_gesamt","lager","gesamt",
                "bedarf_next","verfuegbar_next","differenz",
                "anschaffung","bestellbar","notizen"
            ]
            display_df = df_view[show_cols].copy()
            display_df.columns = [
                "ISBN","Titel","Fach","Klasse","Modus",
                "Umlauf pro Klasse","Umlauf Σ","Lager","Gesamt",
                "Bedarf n.J.","Verfügbar n.J.","Differenz",
                "Anschaffung","Bestellbar","Notizen"
            ]

            def style_row(row):
                if row["Differenz"] < 0:
                    return ["background-color: #ffe0e0"] * len(row)
                elif row["Differenz"] < 5:
                    return ["background-color: #fff3cd"] * len(row)
                return [""] * len(row)

            styled = display_df.style.apply(style_row, axis=1)
            st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Bücher gesamt", len(df_view))
            col_b.metric("Exemplare gesamt", int(df_view["gesamt"].sum()))
            col_c.metric("🔴 Nachbestellen",
                         int(df_view["alarm"].sum()),
                         delta_color="inverse")

    with tab_neu:
        buch_formular(db, existing=None)

    with tab_edit:
        st.subheader("Buch bearbeiten oder löschen")

        if df.empty:
            st.info("Noch keine Bücher vorhanden.")
        else:
            buch_optionen = {
                f"{b['titel']} (ISBN: {b['isbn']})": b
                for b in buecher
            }
            auswahl_label = st.selectbox(
                "Buch auswählen",
                list(buch_optionen.keys()),
                key="edit_select"
            )
            gewaehltes_buch = buch_optionen[auswahl_label]

            col_edit, col_del = st.columns([3, 1])
            with col_edit:
                st.markdown("#### ✏️ Daten bearbeiten")
                buch_formular(db, existing=gewaehltes_buch)

            with col_del:
                st.markdown("#### 🗑️ Buch löschen")
                st.warning(
                    f"**{gewaehltes_buch['titel']}**\n\n"
                    f"ISBN: {gewaehltes_buch['isbn']}\n\n"
                    "Diese Aktion kann nicht rückgängig gemacht werden!"
                )
                confirm = st.checkbox(
                    "Ja, ich möchte dieses Buch unwiderruflich löschen",
                    key="confirm_delete"
                )
                if st.button("🗑️ Endgültig löschen", disabled=not confirm,
                             use_container_width=True, type="primary"):
                    if delete_book(db, gewaehltes_buch["isbn"]):
                        st.success(f"✅ '{gewaehltes_buch['titel']}' wurde gelöscht.")
                        st.session_state["reload"] = True
                        st.rerun()

    with tab_details:
        st.subheader("🔎 Detailansicht")

        if df.empty:
            st.info("Noch keine Bücher vorhanden.")
        else:
            detail_optionen = {
                f"{b['titel']} (ISBN: {b['isbn']})": b
                for b in buecher
            }
            detail_label = st.selectbox(
                "Buch auswählen",
                list(detail_optionen.keys()),
                key="detail_select"
            )
            b = berechne_felder(detail_optionen[detail_label].copy())

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Umlauf gesamt", b["umlauf_gesamt"])
            col2.metric("Lager", b["lager"], help=f"Mindestlagerbestand: {RESERVE} Exemplare")
            col3.metric("Gesamtbestand", b["gesamt"])
            col4.metric(
                "Verfügbar nächstes Jahr",
                b["verfuegbar_next"],
                delta=b["differenz"],
                delta_color="normal",
                help="Bücher die zurückkommen + Lager über Reserve"
            )

            if b["alarm"]:
                st.error(
                    f"⚠️ **NACHBESTELLBEDARF**: Es fehlen **{abs(b['differenz'])} Exemplare**! "
                    f"(Bedarf: {b['bedarf_next']}, Verfügbar: {b['verfuegbar_next']})\n\n"
                    f"Bestellbar im Katalog: **{'Ja' if b['bestellbar'] else 'Nein'}**"
                )
            elif b["differenz"] < RESERVE:
                st.warning(
                    f"🟡 Knappe Reserve: nur {b['differenz']} Exemplare über Bedarf."
                )
            else:
                st.success(
                    f"✅ Bestand ausreichend. Überschuss: {b['differenz']} Exemplare."
                )

            st.markdown("#### Umlauf-Details")
            
            modus = b.get("modus", "einzeln")
            
            if modus == "flexibel":
                st.info("🟢 **Flexibler Umlauf** (jahrgangübergreifend, individuelle Rückgabe)")
                flex = b.get("flex_klassen", {}) or {}
                if flex:
                    flex_data = []
                    for kl, data in sorted(flex.items()):
                        if isinstance(data, dict):
                            flex_data.append({
                                "Klasse": kl,
                                "Im Umlauf": data.get("umlauf", 0),
                                "Kommen zurück": data.get("zurueck", 0),
                                "Behalten": data.get("umlauf", 0) - data.get("zurueck", 0)
                            })
                    if flex_data:
                        flex_df = pd.DataFrame(flex_data)
                        st.dataframe(flex_df, hide_index=True, use_container_width=True)
                        
                        col_sum1, col_sum2, col_sum3 = st.columns(3)
                        col_sum1.metric("Gesamt im Umlauf", sum(d["Im Umlauf"] for d in flex_data))
                        col_sum2.metric("Kommen zurück ↩", b.get("zurueck_gesamt", 0))
                        col_sum3.metric("Behalten", sum(d["Behalten"] for d in flex_data))
                else:
                    st.caption("Keine Klassen eingetragen")
            
            elif modus == "doppel":
                st.info("📘 **Doppeljahrgangs-Buch** (wird 2 Jahre behalten)")
                
                col_jg1, col_jg2 = st.columns(2)
                
                with col_jg1:
                    st.markdown("**🟢 Jahrgang 1** (behalten Bücher)")
                    jg1 = b.get("jahrgang1_klassen") or {}
                    if jg1:
                        jg1_df = pd.DataFrame(
                            [(k, v) for k, v in sorted(jg1.items())],
                            columns=["Klasse", "Exemplare"]
                        )
                        st.dataframe(jg1_df, hide_index=True, use_container_width=True)
                        st.metric("Summe Jahrgang 1", b.get("jahrgang1_gesamt", 0))
                    else:
                        st.caption("Keine Klassen eingetragen")
                
                with col_jg2:
                    st.markdown("**🔴 Jahrgang 2** (geben zurück)")
                    jg2 = b.get("jahrgang2_klassen") or {}
                    if jg2:
                        jg2_df = pd.DataFrame(
                            [(k, v) for k, v in sorted(jg2.items())],
                            columns=["Klasse", "Exemplare"]
                        )
                        st.dataframe(jg2_df, hide_index=True, use_container_width=True)
                        st.metric("Summe Jahrgang 2", b.get("jahrgang2_gesamt", 0))
                    else:
                        st.caption("Keine Klassen eingetragen")
            
            else:
                st.info("📕 **Einzeljahrgangs-Buch**")
                uk = b.get("umlauf_klassen") or {}
                if uk:
                    uk_df = pd.DataFrame(
                        [(k, v) for k, v in sorted(uk.items())],
                        columns=["Klasse", "Exemplare"]
                    )
                    st.dataframe(uk_df, hide_index=True, use_container_width=True)

            st.markdown("#### 📋 Buchdetails")
            meta_col1, meta_col2 = st.columns(2)
            with meta_col1:
                st.markdown(f"**Fach:** {b.get('fach','')}")
                st.markdown(f"**Klasse(n):** {b.get('klasse','')}")
                modus_namen = {'einzeln':'Einzeljahrgang','doppel':'Doppeljahrgang','flexibel':'Flexibler Umlauf'}
                st.markdown(f"**Modus:** {modus_namen.get(modus, modus)}")
                st.markdown(f"**ISBN:** {b.get('isbn','')}")
            with meta_col2:
                st.markdown(f"**Anschaffung:** {b.get('anschaffung','')}")
                st.markdown(f"**Im Katalog bestellbar:** {'✅ Ja' if b.get('bestellbar') else '❌ Nein'}")
                st.markdown(f"**Notizen:** {b.get('notizen','–')}")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def run():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        render_login_page()
    else:
        main_app()


if __name__ == "__main__":
    run()
