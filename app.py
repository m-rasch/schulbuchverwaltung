"""
╔══════════════════════════════════════════════════════════════════╗
║        SCHULBUCHVERWALTUNG  –  Streamlit + Firebase              ║
║        E-Mail/Passwort-Login  |  Firestore Datenbank             ║
║        DOPPELJAHRGANGS-LOGIK FÜR 2-JAHRES-BÜCHER                 ║
╚══════════════════════════════════════════════════════════════════╝

NEUES Datenmodell pro Buch:
  isbn            – Primärschlüssel (String)
  titel           – Buchtitel
  fach            – Schulfach
  klasse          – zugeordnete Klasse(n), z.B. "5/6" oder "5a"
  doppeljahrgang  – True/False (Buch läuft über 2 Jahrgänge)
  
  Für DOPPELJAHRGANGS-BÜCHER:
    jahrgang1_klassen – dict {klasse: anzahl}, z.B. {"5a":28,"5b":27} (behalten Bücher)
    jahrgang2_klassen – dict {klasse: anzahl}, z.B. {"6a":25,"6b":24} (geben zurück)
  
  Für EINJAHRES-BÜCHER:
    umlauf_klassen – dict {klasse: anzahl}
  
  lager           – Anzahl im Lager
  bedarf_next     – erwartete Schülerzahl nächstes Jahr (neue Jahrgang 1)
  anschaffung     – Datum der Anschaffung (String YYYY-MM-DD)
  bestellbar      – True/False (im Schulbuchkatalog verfügbar)
  notizen         – Freitext

Berechnete Felder:
  DOPPELJAHRGANG:
    verfuegbar_next = sum(jahrgang2) + max(lager - 5, 0)
    gesamt = sum(jahrgang1) + sum(jahrgang2) + lager
  
  EINZELJAHRGANG:
    verfuegbar_next = sum(umlauf_klassen) + max(lager - 5, 0)
    gesamt = sum(umlauf_klassen) + lager
  
  differenz = verfuegbar_next - bedarf_next
  alarm = differenz < 0
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

DOPPELJAHRGAENGE = {
    "5/6":  ["5a","5b","5c","6a","6b","6c"],
    "7/8":  ["7a","7b","7c","8a","8b","8c"],
    "9/10": ["9a","9b","9c","10a","10b","10c","10g1","10g2"],
    "11/12":["11/1","11/2","12/1","12/2"],
}

# ═══════════════════════════════════════════════════════════════════════════════
#  FIREBASE – Initialisierung & Datenzugriff
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
            # Felder sicherstellen
            if "doppeljahrgang" in row and row["doppeljahrgang"]:
                if "jahrgang1_klassen" not in row:
                    row["jahrgang1_klassen"] = {}
                if "jahrgang2_klassen" not in row:
                    row["jahrgang2_klassen"] = {}
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
#  BERECHNUNGEN - NEUE LOGIK FÜR DOPPELJAHRGÄNGE
# ═══════════════════════════════════════════════════════════════════════════════

def berechne_felder(buch: dict) -> dict:
    """Berechnete Felder zu einem Buch-Dict hinzufügen."""
    lager  = int(buch.get("lager", 0))
    bedarf = int(buch.get("bedarf_next", 0))
    
    if buch.get("doppeljahrgang", False):
        # DOPPELJAHRGANGS-LOGIK
        jg1 = buch.get("jahrgang1_klassen", {}) or {}
        jg2 = buch.get("jahrgang2_klassen", {}) or {}
        
        jg1_gesamt = sum(int(v) for v in jg1.values())
        jg2_gesamt = sum(int(v) for v in jg2.values())
        umlauf_gesamt = jg1_gesamt + jg2_gesamt
        gesamt = umlauf_gesamt + lager
        
        # NUR Jahrgang 2 kommt zurück (geben Bücher ab)
        reserve_verfuegbar = max(lager - RESERVE, 0)
        verfuegbar_next = jg2_gesamt + reserve_verfuegbar
        
        buch["jahrgang1_gesamt"] = jg1_gesamt
        buch["jahrgang2_gesamt"] = jg2_gesamt
    else:
        # EINZELJAHRGANGS-LOGIK (wie bisher)
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
        
        # Umlauf-Spalte formatieren
        if b.get("doppeljahrgang", False):
            jg1 = b.get("jahrgang1_klassen", {}) or {}
            jg2 = b.get("jahrgang2_klassen", {}) or {}
            jg1_str = ", ".join(f"{k}: {v}" for k, v in sorted(jg1.items()))
            jg2_str = ", ".join(f"{k}: {v}" for k, v in sorted(jg2.items()))
            umlauf_str = f"J1: {jg1_str} | J2: {jg2_str}" if jg1_str and jg2_str else jg1_str or jg2_str
        else:
            uk = b.get("umlauf_klassen", {}) or {}
            umlauf_str = ", ".join(f"{k}: {v}" for k, v in sorted(uk.items()))
        
        rows.append({
            "isbn":            b.get("isbn",""),
            "titel":           b.get("titel",""),
            "fach":            b.get("fach",""),
            "klasse":          b.get("klasse",""),
            "doppeljahrgang":  b.get("doppeljahrgang", False),
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
    lines.append("=" * 110)
    lines.append(f"  SCHULBUCHVERWALTUNG - Export vom {ts}")
    lines.append("=" * 110)

    header = (
        f"{'ISBN':<18} {'Titel':<28} {'Fach':<14} {'Kl.':<8} "
        f"{'Umlf':>5} {'Lag':>5} {'Ges':>5} "
        f"{'Bed.':>5} {'Verf':>5} {'Diff':>5} {'Anschaffung':<12} "
        f"{'Bestell.':<10} {'Notizen':<20}"
    )
    lines.append(header)
    lines.append("-" * 110)

    for _, r in df.iterrows():
        diff_str = str(int(r['differenz']))
        alarm    = " ⚠" if r['alarm'] else ""
        line = (
            f"{str(r['isbn']):<18} {str(r['titel'])[:27]:<28} "
            f"{str(r['fach'])[:13]:<14} {str(r['klasse'])[:7]:<8} "
            f"{int(r['umlauf_gesamt']):>5} {int(r['lager']):>5} "
            f"{int(r['gesamt']):>5} {int(r['bedarf_next']):>5} "
            f"{int(r['verfuegbar_next']):>5} "
            f"{diff_str:>5}{alarm:<2} "
            f"{str(r['anschaffung']):<12} "
            f"{'Ja' if r['bestellbar'] else 'Nein':<10} "
            f"{str(r['notizen'])[:19]:<20}"
        )
        lines.append(line)

    lines.append("=" * 110)
    lines.append(f"  Gesamt: {len(df)} Bücher  |  "
                 f"Alarm (Nachbestellen): {df['alarm'].sum()} Bücher")
    lines.append("=" * 110)
    return "\n".join(lines).encode("utf-8")


def export_pdf(df: pd.DataFrame) -> bytes:
    """PDF-Export mit fpdf2."""
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Schulbuchverwaltung - Bestandsliste", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 6, f"Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')}", 
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    cols   = ["isbn","titel","fach","klasse","umlauf_gesamt","lager",
              "gesamt","bedarf_next","verfuegbar_next","differenz","anschaffung","bestellbar","notizen"]
    labels = ["ISBN","Titel","Fach","Kl.","Umlauf","Lager",
              "Ges.","Bedarf","Verfügbar","Diff","Anschaffung","Bestellb.","Notizen"]
    widths = [25, 45, 20, 10, 12, 10, 10, 12, 12, 10, 22, 14, 45]

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
            str(row["isbn"])[:20],
            str(row["titel"])[:30],
            str(row["fach"])[:14],
            str(row["klasse"])[:8],
            str(int(row["umlauf_gesamt"])),
            str(int(row["lager"])),
            str(int(row["gesamt"])),
            str(int(row["bedarf_next"])),
            str(int(row["verfuegbar_next"])),
            str(int(row["differenz"])),
            str(row["anschaffung"])[:12],
            "Ja" if row["bestellbar"] else "Nein",
            str(row["notizen"])[:30],
        ]
        for val, w in zip(values, widths):
            pdf.cell(w, 6, val, border=1, fill=fill)
        pdf.ln()

    return bytes(pdf.output())


# ═══════════════════════════════════════════════════════════════════════════════
#  FORMULAR: Buch hinzufügen / bearbeiten - NEUE LOGIK
# ═══════════════════════════════════════════════════════════════════════════════

def buch_formular(db, existing: dict | None = None):
    """
    Formular zum Anlegen oder Bearbeiten eines Buches.
    NEUE LOGIK für Doppeljahrgänge mit separaten Eingaben für Jahrgang 1 und 2.
    """
    is_new = existing is None
    if is_new:
        existing = {
            "isbn":"", "titel":"", "fach": ALLE_FAECHER[0],
            "klasse":"", "doppeljahrgang": False,
            "jahrgang1_klassen":{}, "jahrgang2_klassen":{}, "umlauf_klassen":{},
            "lager":0, "bedarf_next":0,
            "anschaffung": str(date.today()),
            "bestellbar": True, "notizen":"",
        }

    with st.form(key=f"buch_form_{'new' if is_new else existing['isbn']}"):
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
                                   help='z.B. "5/6" für Doppeljahrgang oder "7a" für Einzelklasse')
        with c2:
            doppel  = st.checkbox("Doppeljahrgang (Buch wird 2 Jahre behalten)", 
                                  value=existing["doppeljahrgang"])
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

        # BEDINGTE ANZEIGE basierend auf Doppeljahrgang-Checkbox
        if doppel:
            st.markdown("---")
            st.markdown("#### 📘 Umlauf für DOPPELJAHRGANGS-Buch")
            st.info(
                "**Wichtig:** Bei Doppeljahrgangsbüchern gib die Jahrgänge **separat** ein:\n"
                "- **Jahrgang 1** = Schüler die das Buch gerade bekommen haben (behalten es nächstes Jahr)\n"
                "- **Jahrgang 2** = Schüler die das Buch nächstes Jahr abgeben\n\n"
                "**Beispiel Schuljahr 25/26:**\n"
                "- Jahrgang 1: 5a, 5b, 5c (bekommen 25/26, behalten bis 26/27)\n"
                "- Jahrgang 2: 6a, 6b, 6c (haben seit 24/25, geben 26/27 ab)"
            )
            
            col_jg1, col_jg2 = st.columns(2)
            
            with col_jg1:
                st.markdown("**🟢 Jahrgang 1** (behalten Bücher)")
                st.caption("Schüler die das Buch gerade bekommen haben")
                jg1_klassen_str = st.text_input(
                    "Klassen Jahrgang 1",
                    value=", ".join(existing.get("jahrgang1_klassen", {}).keys()),
                    placeholder="z.B. 5a, 5b, 5c",
                    key=f"jg1_klassen_{isbn if not is_new else 'new'}"
                )
                
                jg1_new = {}
                if jg1_klassen_str.strip():
                    jg1_klassen_list = [k.strip() for k in re.split(r"[,;]+", jg1_klassen_str) if k.strip()]
                    jg1_existing = existing.get("jahrgang1_klassen", {}) or {}
                    
                    for kl in jg1_klassen_list:
                        val = st.number_input(
                            f"Klasse {kl}",
                            min_value=0,
                            value=int(jg1_existing.get(kl, 0)),
                            key=f"jg1_{isbn if not is_new else 'new'}_{kl}"
                        )
                        if val > 0:
                            jg1_new[kl] = val
            
            with col_jg2:
                st.markdown("**🔴 Jahrgang 2** (geben zurück)")
                st.caption("Schüler die das Buch nächstes Jahr abgeben")
                jg2_klassen_str = st.text_input(
                    "Klassen Jahrgang 2",
                    value=", ".join(existing.get("jahrgang2_klassen", {}).keys()),
                    placeholder="z.B. 6a, 6b, 6c",
                    key=f"jg2_klassen_{isbn if not is_new else 'new'}"
                )
                
                jg2_new = {}
                if jg2_klassen_str.strip():
                    jg2_klassen_list = [k.strip() for k in re.split(r"[,;]+", jg2_klassen_str) if k.strip()]
                    jg2_existing = existing.get("jahrgang2_klassen", {}) or {}
                    
                    for kl in jg2_klassen_list:
                        val = st.number_input(
                            f"Klasse {kl}",
                            min_value=0,
                            value=int(jg2_existing.get(kl, 0)),
                            key=f"jg2_{isbn if not is_new else 'new'}_{kl}"
                        )
                        if val > 0:
                            jg2_new[kl] = val
            
            # Berechnung anzeigen
            jg1_summe = sum(jg1_new.values())
            jg2_summe = sum(jg2_new.values())
            verf_next = jg2_summe + max(lager - RESERVE, 0)
            diff = verf_next - bedarf
            
            st.markdown("---")
            st.markdown("**📊 Berechnung für nächstes Jahr:**")
            col_calc1, col_calc2, col_calc3, col_calc4 = st.columns(4)
            col_calc1.metric("Jahrgang 1 (behalten)", jg1_summe)
            col_calc2.metric("Jahrgang 2 (zurück)", jg2_summe)
            col_calc3.metric("Verfügbar nächstes Jahr", verf_next, 
                           help=f"Jahrgang 2 ({jg2_summe}) + Lager über Reserve ({max(lager-RESERVE, 0)})")
            col_calc4.metric("Differenz", diff, 
                           delta_color="inverse" if diff < 0 else "normal")
            
            if diff < 0:
                st.error(f"⚠️ Es fehlen {abs(diff)} Bücher für nächstes Jahr!")
        
        else:
            # EINZELJAHRGANGS-LOGIK (wie bisher)
            st.markdown("---")
            st.markdown("#### 📕 Umlauf (Einzeljahrgang)")
            st.caption("Trage ein, wie viele Exemplare aktuell in welcher Klasse im Umlauf sind.")

            klassen_str = st.text_input(
                "Klassen im Umlauf",
                value=", ".join(existing.get("umlauf_klassen", {}).keys()) if not doppel else "",
                placeholder="z.B. 7a, 7b, 7c",
                key=f"umlauf_klassen_str_{isbn if not is_new else 'new'}"
            )
            
            uk_new = {}
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
                            key=f"uk_{isbn if not is_new else 'new'}_{kl}"
                        )
                        if val > 0:
                            uk_new[kl] = val
            
            jg1_new = {}
            jg2_new = {}

        notizen = st.text_area("Notizen", value=existing.get("notizen",""),
                               placeholder='z.B. "aussortiert ab 2026"')

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
            "doppeljahrgang": doppel,
            "lager":          lager,
            "bedarf_next":    bedarf,
            "anschaffung":    anschaffung.strip(),
            "bestellbar":     bestellbar,
            "notizen":        notizen.strip(),
        }
        
        if doppel:
            buch["jahrgang1_klassen"] = jg1_new
            buch["jahrgang2_klassen"] = jg2_new
            buch["umlauf_klassen"] = {}  # Leer für Doppeljahrgänge
        else:
            buch["umlauf_klassen"] = uk_new
            buch["jahrgang1_klassen"] = {}  # Leer für Einzeljahrgänge
            buch["jahrgang2_klassen"] = {}
        
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
            pdf_bytes = export_pdf(df)
            st.download_button(
                "⬇️ Export als PDF",
                data      = pdf_bytes,
                file_name = f"schulbuecher_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime      = "application/pdf",
                use_container_width=True,
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
                "isbn","titel","fach","klasse",
                "umlauf_gesamt","lager","verfuegbar_next","bedarf_next","differenz",
                "bestellbar","notizen"
            ]].copy()
            alarm_show.columns = [
                "ISBN","Titel","Fach","Klasse",
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
                "isbn","titel","fach","klasse","doppeljahrgang",
                "umlauf_klassen","umlauf_gesamt","lager","gesamt",
                "bedarf_next","verfuegbar_next","differenz",
                "anschaffung","bestellbar","notizen"
            ]
            display_df = df_view[show_cols].copy()
            display_df.columns = [
                "ISBN","Titel","Fach","Klasse","Doppeljahrgang",
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
        st.subheader("🔎 Detailansicht & Jahresplanung")

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
            col2.metric("Lager", b["lager"],
                        help=f"Mindestlagerbestand: {RESERVE} Exemplare")
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
                    f"🟡 Knappe Reserve: nur {b['differenz']} Exemplare über Bedarf. "
                    f"Ggf. nachbestellen."
                )
            else:
                st.success(
                    f"✅ Bestand ausreichend. Überschuss: {b['differenz']} Exemplare."
                )

            st.markdown("#### Umlauf-Details")
            
            if b.get("doppeljahrgang", False):
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
                        columns=["Klasse", "Exemplare im Umlauf"]
                    )
                    c1, c2 = st.columns([1, 2])
                    with c1:
                        st.dataframe(uk_df, hide_index=True, use_container_width=True)
                    with c2:
                        fig2 = px.bar(
                            uk_df, x="Klasse", y="Exemplare im Umlauf",
                            color="Exemplare im Umlauf",
                            color_continuous_scale="Blues",
                            title=f"Umlauf: {b['titel']}",
                        )
                        fig2.update_layout(showlegend=False,
                                           plot_bgcolor="rgba(0,0,0,0)")
                        st.plotly_chart(fig2, use_container_width=True)

            st.markdown("#### 📋 Buchdetails")
            meta_col1, meta_col2 = st.columns(2)
            with meta_col1:
                st.markdown(f"**Fach:** {b.get('fach','')}")
                st.markdown(f"**Klasse(n):** {b.get('klasse','')}")
                st.markdown(f"**Doppeljahrgang:** {'Ja' if b.get('doppeljahrgang') else 'Nein'}")
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
