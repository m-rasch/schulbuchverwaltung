"""
╔══════════════════════════════════════════════════════════════════╗
║        SCHULBUCHVERWALTUNG  –  Streamlit + Firebase              ║
║        E-Mail/Passwort-Login  |  Firestore Datenbank             ║
╚══════════════════════════════════════════════════════════════════╝

Datenmodell pro Buch:
  isbn            – Primärschlüssel (String)
  titel           – Buchtitel
  fach            – Schulfach
  klasse          – zugeordnete Klasse(n), z.B. "5a, 5b, 5c"
  doppeljahrgang  – True/False (Buch läuft über 2 Jahrgänge)
  umlauf_klassen  – dict {klasse: anzahl}, z.B. {"5a":28,"5b":27,"5c":26}
  lager           – Anzahl im Lager
  bedarf_next     – erwartete Schülerzahl nächstes Jahr (manuell)
  anschaffung     – Datum der Anschaffung (String YYYY-MM-DD)
  bestellbar      – True/False (im Schulbuchkatalog verfügbar)
  notizen         – Freitext

Berechnete Felder (nicht gespeichert):
  gesamt          = sum(umlauf_klassen.values()) + lager
  umlauf_gesamt   = sum(umlauf_klassen.values())
  verfuegbar_next = umlauf_gesamt (kommen zurück) + max(lager - 5, 0)
  differenz       = verfuegbar_next - bedarf_next
  alarm           = differenz < 0
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
    "Physik","Chemie","Biologie","Geographie","Geschichte","Politik",
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
        # Zeilenumbrüche im Private Key wiederherstellen
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
            # umlauf_klassen sicherstellen
            if "umlauf_klassen" not in row or not isinstance(row["umlauf_klassen"], dict):
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
#  BERECHNUNGEN
# ═══════════════════════════════════════════════════════════════════════════════

def berechne_felder(buch: dict) -> dict:
    """Berechnete Felder zu einem Buch-Dict hinzufügen."""
    uk = buch.get("umlauf_klassen", {}) or {}
    umlauf_gesamt = sum(int(v) for v in uk.values())
    lager         = int(buch.get("lager", 0))
    bedarf        = int(buch.get("bedarf_next", 0))
    gesamt        = umlauf_gesamt + lager

    # Verfügbar fürs nächste Jahr:
    # alle zurückkommenden Bücher + was im Lager über die Reserve von 5 hinausgeht
    reserve_verfuegbar = max(lager - RESERVE, 0)
    verfuegbar_next    = umlauf_gesamt + reserve_verfuegbar
    differenz          = verfuegbar_next - bedarf

    buch["umlauf_gesamt"]    = umlauf_gesamt
    buch["gesamt"]           = gesamt
    buch["verfuegbar_next"]  = verfuegbar_next
    buch["differenz"]        = differenz
    buch["alarm"]            = differenz < 0
    return buch


def buecher_zu_df(buecher: list[dict]) -> pd.DataFrame:
    """Liste von Buch-Dicts → übersichtlicher DataFrame."""
    rows = []
    for b in buecher:
        b = berechne_felder(b)
        uk_str = ", ".join(
            f"{k}: {v}" for k, v in sorted((b.get("umlauf_klassen") or {}).items())
        )
        rows.append({
            "isbn":            b.get("isbn",""),
            "titel":           b.get("titel",""),
            "fach":            b.get("fach",""),
            "klasse":          b.get("klasse",""),
            "doppeljahrgang":  b.get("doppeljahrgang", False),
            "umlauf_klassen":  uk_str,
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
#  AUTHENTIFIZIERUNG (einfaches E-Mail/Passwort über st.secrets)
# ═══════════════════════════════════════════════════════════════════════════════

def check_login(email: str, password: str) -> bool:
    """
    Prüft gegen eine Nutzerliste in secrets.toml:
      [users]
      "lehrer@schule.de" = "meinpasswort"
    """
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
    lines.append(f"  SCHULBUCHVERWALTUNG – Export vom {ts}")
    lines.append("=" * 110)

    col_widths = {
        "isbn": 18, "titel": 28, "fach": 14, "klasse": 8,
        "umlauf_gesamt": 7, "lager": 6, "gesamt": 7,
        "bedarf_next": 7, "differenz": 7, "anschaffung": 12,
        "bestellbar": 10, "notizen": 20,
    }
    header = (
        f"{'ISBN':<18} {'Titel':<28} {'Fach':<14} {'Kl.':<8} "
        f"{'Umlf':>5} {'Lag':>5} {'Ges':>5} "
        f"{'Bed.':>5} {'Diff':>5} {'Anschaffung':<12} "
        f"{'Bestell.':<10} {'Notizen':<20}"
    )
    lines.append(header)
    lines.append("-" * 110)

    for _, r in df.iterrows():
        diff_str = str(r['differenz'])
        alarm    = " ⚠" if r['alarm'] else ""
        line = (
            f"{str(r['isbn']):<18} {str(r['titel'])[:27]:<28} "
            f"{str(r['fach'])[:13]:<14} {str(r['klasse'])[:7]:<8} "
            f"{int(r['umlauf_gesamt']):>5} {int(r['lager']):>5} "
            f"{int(r['gesamt']):>5} {int(r['bedarf_next']):>5} "
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
    pdf.cell(0, 10, "Schulbuchverwaltung – Bestandsliste", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 6, f"Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
             ln=True, align="C")
    pdf.ln(3)

    cols   = ["isbn","titel","fach","klasse","umlauf_gesamt","lager",
              "gesamt","bedarf_next","differenz","anschaffung","bestellbar","notizen"]
    labels = ["ISBN","Titel","Fach","Kl.","Umlauf","Lager",
              "Ges.","Bedarf","Diff","Anschaffung","Bestellb.","Notizen"]
    widths = [28, 48, 22, 12, 14, 12, 12, 14, 12, 24, 16, 50]

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
#  FORMULAR: Buch hinzufügen / bearbeiten
# ═══════════════════════════════════════════════════════════════════════════════

def buch_formular(db, existing: dict | None = None):
    """
    Formular zum Anlegen oder Bearbeiten eines Buches.
    existing=None → neues Buch; existing=dict → Bearbeiten.
    """
    is_new = existing is None
    if is_new:
        existing = {
            "isbn":"", "titel":"", "fach": ALLE_FAECHER[0],
            "klasse":"", "doppeljahrgang": False,
            "umlauf_klassen":{}, "lager":0, "bedarf_next":0,
            "anschaffung": str(date.today()),
            "bestellbar": True, "notizen":"",
        }

    with st.form(key=f"buch_form_{'new' if is_new else existing['isbn']}"):
        st.markdown("### " + ("➕ Neues Buch anlegen" if is_new else f"✏️ Bearbeiten: {existing.get('titel','')}"))

        c1, c2 = st.columns(2)
        with c1:
            isbn   = st.text_input("ISBN *", value=existing["isbn"],
                                   disabled=not is_new,
                                   help="ISBN ist der eindeutige Schlüssel und kann nach dem Anlegen nicht mehr geändert werden.")
            titel  = st.text_input("Titel *", value=existing["titel"])
            fach_idx = ALLE_FAECHER.index(existing["fach"]) if existing["fach"] in ALLE_FAECHER else 0
            fach   = st.selectbox("Fach", ALLE_FAECHER, index=fach_idx)
            klasse = st.text_input("Zugeordnete Klasse(n)",
                                   value=existing["klasse"],
                                   help='z.B. "5a, 5b, 5c" oder "7/8" für Doppeljahrgang')
        with c2:
            doppel  = st.checkbox("Doppeljahrgang", value=existing["doppeljahrgang"])
            lager   = st.number_input("Lager (Exemplare)", min_value=0,
                                      value=int(existing.get("lager", 0)))
            bedarf  = st.number_input("Bedarf nächstes Jahr", min_value=0,
                                      value=int(existing.get("bedarf_next", 0)))
            anschaffung = st.text_input("Anschaffungsdatum (JJJJ-MM-TT)",
                                        value=existing.get("anschaffung",""),
                                        placeholder="2023-08-01")
            bestellbar  = st.checkbox("Im Schulbuchkatalog bestellbar",
                                      value=existing.get("bestellbar", True))

        st.markdown("#### Umlauf pro Klasse")
        st.caption("Trage ein, wie viele Exemplare aktuell in welcher Klasse im Umlauf sind.")

        # Aktuelle umlauf_klassen
        uk_existing = existing.get("umlauf_klassen", {}) or {}
        # Welche Klassen vorbelegen?
        klassen_vorschlag = []
        if klasse:
            klassen_vorschlag = [k.strip() for k in re.split(r"[,;]+", klasse) if k.strip()]
        # Alle bisher eingetragenen Klassen + Vorschlag anzeigen
        alle_uk_klassen = sorted(set(list(uk_existing.keys()) + klassen_vorschlag))

        uk_new = {}
        if alle_uk_klassen:
            n_cols = min(len(alle_uk_klassen), 4)
            cols   = st.columns(n_cols)
            for i, kl in enumerate(alle_uk_klassen):
                with cols[i % n_cols]:
                    val = st.number_input(
                        f"Klasse {kl}", min_value=0,
                        value=int(uk_existing.get(kl, 0)),
                        key=f"uk_{isbn if not is_new else 'new'}_{kl}"
                    )
                    if val > 0:
                        uk_new[kl] = val
        else:
            st.info("Klasse(n) im Feld 'Zugeordnete Klasse(n)' eintragen, um den Umlauf pro Klasse zu erfassen.")

        # Zusätzliche Klasse
        extra_kl = st.selectbox("Weitere Klasse hinzufügen", ["– keine –"] + ALLE_KLASSEN,
                                 key=f"extra_kl_{'new' if is_new else existing['isbn']}")
        if extra_kl != "– keine –" and extra_kl not in uk_new:
            uk_new[extra_kl] = 0

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
            "umlauf_klassen": uk_new,
            "lager":          lager,
            "bedarf_next":    bedarf,
            "anschaffung":    anschaffung.strip(),
            "bestellbar":     bestellbar,
            "notizen":        notizen.strip(),
        }
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
    # ── Seiten-Konfiguration ──────────────────────────────────────────────────
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📚",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Firebase ──────────────────────────────────────────────────────────────
    db = init_firebase()

    # ── Daten laden ───────────────────────────────────────────────────────────
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

        # TXT-Backup
        fname_txt = f"schulbuch_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        st.download_button(
            "⬇️ Backup als TXT",
            data    = export_txt(df) if not df.empty else b"Keine Daten",
            file_name = fname_txt,
            mime    = "text/plain",
            use_container_width=True,
        )

        # PDF
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
