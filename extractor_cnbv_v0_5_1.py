# -*- coding: utf-8 -*-
"""
Extractor CNBV PDFs -> MySQL (v0.5.1)

Novedades sobre v0.5 (solo 'modificaciones'):
- nombre_regulacion (SOLO 1ª hoja) acepta además:
  • "Resolución modificatoria ..."
  • "Resolución que modifica ..."
- Sigue aceptando: "Ley General de ...", "Reglamento de ...",
  "Disposiciones de carácter general ...", "Lineamientos para ...",
  "Acuerdo por el que ...", "Constitución ...".
- Ensamble de título multi-línea más robusto (corta en líneas genéricas
  del DOF, "Artículo ...", "Transitorio ...", o tras 6 líneas).

Resto:
- Tabla 'modificaciones' (sin id_articulo); inserta 1 fila por PDF:
  nombre_regulacion (patrón anterior), texto_modificacion (TODO el PDF),
  fecha_publicacion (texto o AAAAMMDD en nombre), ruta_archivo=NULL, embedding_completo=NULL.
- Documentos+artículos (incluye Transitorios).
"""

from __future__ import annotations
import os, re
from typing import Optional, List, Tuple
from dataclasses import dataclass
from datetime import date

import fitz  # PyMuPDF
import mysql.connector as mysql

# ==========================
# CONFIG
# ==========================
DRY_RUN = False  # True = no escribe en la base

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "1234"),
    "database": os.getenv("MYSQL_DATABASE", "buscador_normativo"),
    "autocommit": False,
}

BASE_DIR = r"C:\Users\kire4\OneDrive\Documentos\Buscador inteligente\Prog_nom\Normatividad"
DIR_COMPULSADAS    = os.path.join(BASE_DIR, "Versiones compulsadas")
DIR_MODIFICACIONES = os.path.join(BASE_DIR, "Modificaciones")
DIR_ANEXOS         = os.path.join(BASE_DIR, "Anexos vigentes")
print("DIR_COMPULSADAS:", DIR_COMPULSADAS)
print("DIR_MODIFICACIONES:", DIR_MODIFICACIONES)
print("DIR_ANEXOS:", DIR_ANEXOS)

# ==========================
# UTILIDADES: fechas, tipos, helpers
# ==========================
MESES = {
  "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
  "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
  "noviembre":11,"diciembre":12
}

FECHA_LARGA = r"(?P<d>\d{1,2})\s+de\s+(?P<m>enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+de\s+(?P<y>\d{4})"
FECHA_NUM   = r"(?P<d>\d{1,2})[-/](?P<m>\d{1,2})[-/](?P<y>\d{4})"
FECHA_NUM_2 = r"(?P<d2>\d{1,2})[-/](?P<m2>\d{1,2})[-/](?P<y2>\d{4})"

VIGENTE_REFORMA = re.compile(
    rf"(?i)texto\s+vigente.*?\xFAltima\s+reforma\s+publicada\s+dof.*?(?:\[\s*{FECHA_NUM}\s*\]|{FECHA_NUM_2})"
)
DOF_ENCABEZADO  = re.compile(rf"(?i)(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)?\s*{FECHA_LARGA}")
FECHAS_LARGAS_TXT = re.compile(FECHA_LARGA, re.IGNORECASE)
FECHAS_NUM_TXT    = re.compile(FECHA_NUM)

FECHA_NOMBRE = re.compile(r"(?i)\b(\d{4})(\d{2})(\d{2})\b")

ROMAN = {"I":1,"V":5,"X":10,"L":50,"C":100,"D":500,"M":1000}

TIPO_PAL_MAP = [
  (re.compile(r"\bley\b", re.I), "Ley"),
  (re.compile(r"\breglamento\b", re.I), "Reglamento"),
  (re.compile(r"\bconstituci[oó]n\b", re.I), "Constitución"),
  (re.compile(r"\bdisposiciones?\b", re.I), "Disposiciones"),
  (re.compile(r"\blineamientos?\b", re.I), "Lineamientos"),
  (re.compile(r"\bacuerdo\b", re.I), "Acuerdo"),
  (re.compile(r"\bresoluci[oó]n\b", re.I), "Resolución"),
  (re.compile(r"\bnorma\s+oficial\s+mexicana\b", re.I), "Norma Oficial Mexicana"),
  (re.compile(r"\bmanual\b", re.I), "Manual"),
  (re.compile(r"\bdecreto\b", re.I), "Decreto"),
  (re.compile(r"\baviso\b", re.I), "Aviso"),
  (re.compile(r"\bconvocatoria\b", re.I), "Convocatoria"),
  (re.compile(r"\bconvenio\b", re.I), "Convenio"),
  (re.compile(r"\bprocedimiento\b", re.I), "Procedimiento"),
  (re.compile(r"\bprograma\b", re.I), "Programa"),
  (re.compile(r"\breglas\b", re.I), "Reglas"),
  (re.compile(r"\bc[oó]digo\b", re.I), "Código"),
]

def roman_to_int(s: str) -> int:
    s = s.upper()
    total = 0
    prev = 0
    for ch in reversed(s):
        val = ROMAN.get(ch, 0)
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    return total

def normaliza_id_articulo(raw: str) -> str:
    s = raw.strip()
    s = re.sub(r"[º°]\.?", "", s)
    s = re.sub(r"\bo\.\b", "", s, flags=re.I).strip()
    suf = ""
    m = re.search(r"(?:[-–]\s*)?(Bis|Ter|Qu(?:á|a)ter|Quinquies|Sexies|Septies|Octies|Nonies|Decies|[A-Za-z])$", s, re.I)
    if m:
        suf = "-" + m.group(1).upper()
        s = re.sub(r"(?:[-–]\s*)?(Bis|Ter|Qu(?:á|a)ter|Quinquies|Sexies|Septies|Octies|Nonies|Decies|[A-Za-z])$", "", s, flags=re.I).strip()
    if re.fullmatch(r"[IVXLCDM]+", s, re.I):
        base = str(roman_to_int(s))
        return base + suf
    num = re.search(r"\d{1,4}", s)
    if num:
        return num.group(0) + suf
    return s + suf

# ---------- PDF -> texto
def texto_pdf(path: str) -> Tuple[str, str]:
    with fitz.open(path) as doc:
        primera = doc[0].get_text("text") if doc.page_count else ""
        full = "\n".join(p.get_text("text") for p in doc)
        return primera, full

# ---------- TÍTULO PARA DOCUMENTOS/ANEXOS (flexible 1-3 páginas)
CLAVES_NORM = ("disposiciones", "ley", "reglamento", "lineamientos", "resolución", "resolucion", "acuerdo", "código", "codigo")
STOP_LINE_GENERIC = re.compile(r"(?i)^(?:índice|indice|al\s+margen|al\s+marg[eé]n|publicad[oa]s?\s+en|diario\s+oficial)")

def detectar_nombre_regulacion_documento(texto_primera: str, texto_full: str) -> Optional[str]:
    def _scan(lines: List[str]) -> Optional[str]:
        acc = []
        for s in lines:
            st = s.strip()
            if not st:
                if acc: break
                continue
            if STOP_LINE_GENERIC.search(st):
                if acc: break
                else: continue
            letters = [c for c in st if c.isalpha()]
            upper_ratio = (sum(1 for c in letters if c.upper() == c) / len(letters)) if letters else 0.0
            if upper_ratio > 0.6 or any(k in st.lower() for k in CLAVES_NORM):
                acc.append(st)
            elif acc:
                break
        if acc:
            titulo = " ".join(" ".join(acc).split())
            if "diario oficial" in titulo.lower():
                return None
            return titulo
        return None
    t = _scan(texto_primera.splitlines())
    if t: return t
    return _scan(texto_full[:12000].splitlines())

# ---------- TÍTULO PARA MODIFICACIONES (SOLO 1ª HOJA, patrones pedidos + Resolución*)
TITLE_START_MOD = re.compile(
    r"(?i)^\s*(?:"
    r"Ley\s+General\s+de\b|"
    r"Reglamento\s+de\b|"
    r"Disposiciones\s+de\s+car[aá]cter\s+general\b|"
    r"Lineamientos\s+para\b|"
    r"Acuerdo\s+por\s+el\s+que\b|"
    r"Constituci[oó]n\b|"
    r"Resoluci[oó]n\s+modificatoria\b|"
    r"Resoluci[oó]n\s+que\s+modifica\b"
    r")"
)

STOP_LINE_MOD = re.compile(
    r"(?i)^(?:índice|indice|al\s+margen|al\s+marg[eé]n|publicad[oa]s?\s+en|diario\s+oficial|\(?\s*primera\s+secci[oó]n\s*\)?|\(?\s*segunda\s+secci[oó]n\s*\)?|\(?\s*tercera\s+secci[oó]n\s*\)?)"
)
BREAK_BODY = re.compile(r"(?i)^\s*(art[íi]culo|art\.)\b")
BREAK_TRANS = re.compile(r"(?i)^\s*transitorio(?:s)?\b")

def detectar_nombre_norma_por_patron_modificaciones(texto_primera: str) -> Optional[str]:
    """
    Solo 1ª hoja: encuentra la primera línea que empieza con TITLE_START_MOD,
    junta las siguientes líneas del bloque (máx 6) mientras no sean STOP/BREAK.
    """
    lines = [ln.strip() for ln in texto_primera.splitlines()]
    n = len(lines)
    for i, s in enumerate(lines):
        if not s: continue
        if STOP_LINE_MOD.search(s): continue
        if TITLE_START_MOD.search(s):
            bloque = [s]
            j = i + 1
            while j < n and len(bloque) < 6:
                nxt = lines[j].strip()
                if not nxt or STOP_LINE_MOD.search(nxt) or BREAK_BODY.search(nxt) or BREAK_TRANS.search(nxt):
                    break
                # si es claramente cuerpo (minúsculas predominan) y no parece una continuación, corta
                letters = [c for c in nxt if c.isalpha()]
                upper_ratio = (sum(1 for c in letters if c.upper() == c) / len(letters)) if letters else 0.0
                if upper_ratio < 0.45 and not TITLE_START_MOD.search(nxt):
                    # permite una línea más si termina el título con punto
                    bloque.append(nxt)
                    break
                bloque.append(nxt)
                j += 1
            return " ".join(" ".join(bloque).split())
    return None

def tipo_desde_nombre(nombre: str) -> str:
    if not nombre: return "Otros"
    for rx, tipo in TIPO_PAL_MAP:
        if rx.search(nombre): return tipo
    return "Otros"

# ---------- Fechas
def _to_date(d: str, m: str, y: str, largo: bool) -> Optional[date]:
    try:
        if largo:
            mm = MESES[m.lower()]
            return date(int(y), mm, int(d))
        return date(int(y), int(m), int(d))
    except Exception:
        return None

def extraer_fechas(texto: str) -> List[date]:
    fechas: List[date] = []
    for m in VIGENTE_REFORMA.finditer(texto):
        gd = m.groupdict()
        if gd.get("d") and gd.get("m") and gd.get("y"):
            f = _to_date(gd["d"], gd["m"], gd["y"], largo=False)
        elif gd.get("d2") and gd.get("m2") and gd.get("y2"):
            f = _to_date(gd["d2"], gd["m2"], gd["y2"], largo=False)
        else:
            f = None
        if f: fechas.append(f)
    for m in DOF_ENCABEZADO.finditer(texto):
        gd = m.groupdict(); f = _to_date(gd["d"], gd["m"], gd["y"], largo=True)
        if f: fechas.append(f)
    for m in FECHAS_LARGAS_TXT.finditer(texto):
        gd = m.groupdict(); f = _to_date(gd["d"], gd["m"], gd["y"], largo=True)
        if f: fechas.append(f)
    for m in FECHAS_NUM_TXT.finditer(texto):
        gd = m.groupdict(); f = _to_date(gd["d"], gd["m"], gd["y"], largo=False)
        if f: fechas.append(f)
    return fechas

def fecha_publicacion_mas_reciente(texto: str) -> Optional[date]:
    fs = extraer_fechas(texto)
    return max(fs) if fs else None

def parse_fecha_from_filename(filename: str) -> Optional[date]:
    m = FECHA_NOMBRE.search(filename)
    if not m: return None
    y, mm, dd = m.group(1), m.group(2), m.group(3)
    try:
        return date(int(y), int(mm), int(dd))
    except Exception:
        return None

# ==========================
# ARTÍCULOS (incluye Transitorios)
# ==========================
HEAD_RE = re.compile(
    r"(?im)^\s*(?:Art[íi]culo|Art\.)\s+"
    r"(\d{1,4}(?:\s*(?:Bis|Ter|Qu(?:á|a)ter|Quinquies|Sexies|Septies|Octies|Nonies|Decies|[A-Za-z]))?)"
    r"\s*[\.\-–—:]{1,2}\s+"
)

TRANS_TIT_LINE = re.compile(r"(?im)^\s*(?:art[íi]culos?\s+)?transitorio(?:s)?\s*:?\s*$")
TRANS_TOK      = re.compile(r"(?i)\btransitorio(?:s)?\b")
TRANSITORIO_ITEM_RE = re.compile(
    r"(?im)^\s*(Único|Unico|"
    r"(?:Primero|Segundo|Tercero|Cuarto|Quinto|Sexto|Séptimo|Septimo|Octavo|Noveno|Décimo|Decimo|Undécimo|Undecimo|Duodécimo|Duodecimo)|"
    r"[IVXLCDM]+|\d{1,3})\s*(?:\.\-|\.-|[\.\-–—:])\s+"
)

ORD_MAP = {
  "unico":"Único","único":"Único",
  "primero":"Primero","segundo":"Segundo","tercero":"Tercero","cuarto":"Cuarto","quinto":"Quinto","sexto":"Sexto",
  "séptimo":"Séptimo","septimo":"Séptimo","octavo":"Octavo","noveno":"Noveno","décimo":"Décimo","decimo":"Décimo",
  "undécimo":"Undécimo","undecimo":"Undécimo","duodécimo":"Duodécimo","duodecimo":"Duodécimo",
}

def _normaliza_trans_ord(s: str) -> str:
    t = s.strip(); low = t.lower()
    if low in ORD_MAP: return ORD_MAP[low]
    if re.fullmatch(r"[ivxlcdm]+", low):
        val = {"i":1,"v":5,"x":10,"l":50,"c":100,"d":500,"m":1000}; tot, prev = 0, 0
        for ch in low[::-1]:
            v = val[ch]; tot = tot - v if v < prev else tot + v; prev = max(prev, v)
        mapa = {1:"Primero",2:"Segundo",3:"Tercero",4:"Cuarto",5:"Quinto",6:"Sexto",7:"Séptimo",8:"Octavo",9:"Noveno",10:"Décimo",
                11:"Undécimo",12:"Duodécimo"}
        return mapa.get(tot, str(tot))
    if re.fullmatch(r"\d{1,3}", low):
        n = int(low)
        mapa = {1:"Primero",2:"Segundo",3:"Tercero",4:"Cuarto",5:"Quinto",6:"Sexto",7:"Séptimo",8:"Octavo",9:"Noveno",10:"Décimo",
                11:"Undécimo",12:"Duodécimo"}
        return mapa.get(n, str(n))
    return t.capitalize()

def _partir_transitorios(texto: str, start_idx: int) -> List[Tuple[str, str]]:
    sub = texto[start_idx:]
    matches = list(TRANSITORIO_ITEM_RE.finditer(sub))
    res: List[Tuple[str, str]] = []
    for i, m in enumerate(matches):
        ini = m.end()
        fin = matches[i+1].start() if i+1 < len(matches) else len(sub)
        ordinal = _normaliza_trans_ord(m.group(1))
        numero = f"Transitorio {ordinal}"
        cuerpo = sub[ini:fin].strip()
        if cuerpo: res.append((numero, cuerpo))
    return res

def partir_articulos(texto: str) -> List[Tuple[str, str]]:
    heads = list(HEAD_RE.finditer(texto))
    normales: List[Tuple[str, str]] = []
    for i, m in enumerate(heads):
        ini = m.end()
        fin = heads[i+1].start() if i+1 < len(heads) else len(texto)
        raw = m.group(1)
        s = raw.strip()
        s = re.sub(r"[º°]\.?", "", s)
        suf = ""
        x = re.search(r"(?:[-–]\s*)?(Bis|Ter|Qu(?:á|a)ter|Quinquies|Sexies|Septies|Octies|Nonies|Decies|[A-Za-z])$", s, re.I)
        if x:
            suf = "-" + x.group(1).upper()
            s = re.sub(r"(?:[-–]\s*)?(Bis|Ter|Qu(?:á|a)ter|Quinquies|Sexies|Septies|Octies|Nonies|Decies|[A-Za-z])$", "", s, flags=re.I).strip()
        if re.fullmatch(r"[IVXLCDM]+", s, re.I):
            base = str(roman_to_int(s)); numero = base + suf
        else:
            mnum = re.search(r"\d{1,4}", s); numero = (mnum.group(0) if mnum else s) + suf
        cuerpo = texto[ini:fin].strip()
        if cuerpo: normales.append((numero, cuerpo))

    mline = TRANS_TIT_LINE.search(texto)
    if mline:
        start_idx = mline.end()
    else:
        mword = TRANS_TOK.search(texto)
        if mword:
            start_idx = mword.start()
        else:
            first_item = TRANSITORIO_ITEM_RE.search(texto)
            start_idx = first_item.start() if first_item else None

    trans = _partir_transitorios(texto, start_idx) if start_idx is not None else []
    return normales + trans

# ==========================
# DB
# ==========================
class DB:
    def __init__(self, cfg: dict):
        self.cn = mysql.connect(**cfg)
        self.cur = self.cn.cursor(dictionary=True)

    def commit(self): self.cn.commit()
    def rollback(self): self.cn.rollback()
    def close(self):
        try: self.cur.close()
        finally: self.cn.close()

    # documentos
    def insert_documento(self, nombre: Optional[str], ambito: str, tipo: str,
                         fecha_pub: Optional[date], emisor: str) -> int:
        if DRY_RUN:
            print("[DRY RUN] insert_documento", nombre, ambito, tipo, fecha_pub, emisor); 
            return -1
        sql = (
            "INSERT INTO documentos (nombre_regulacion, ambito_aplicacion, tipo_de_ordenamiento, "
            "fecha_publicacion, emisor, ruta_archivo, embedding_completo, embedding_nombre, embedding_ambito, embedding_tipo, embedding_emisor) "
            "VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,NULL,NULL,NULL)"
        )
        self.cur.execute(sql, [nombre, ambito, tipo, fecha_pub, emisor, None])
        return self.cur.lastrowid

    def find_documento_by_fecha(self, fecha_pub: date) -> Optional[int]:
        self.cur.execute(
            "SELECT id_documento FROM documentos WHERE fecha_publicacion = %s ORDER BY id_documento DESC LIMIT 1",
            [fecha_pub],
        )
        r = self.cur.fetchone()
        return r["id_documento"] if r else None

    def find_documento_mas_reciente_por_normativa(self, key: str) -> Optional[int]:
        like = f"%{key}%"
        self.cur.execute(
            "SELECT id_documento FROM documentos WHERE (nombre_regulacion LIKE %s) ORDER BY fecha_publicacion DESC, id_documento DESC LIMIT 1",
            [like],
        )
        r = self.cur.fetchone()
        if r: return r["id_documento"]
        self.cur.execute(
            "SELECT id_documento FROM documentos ORDER BY fecha_publicacion DESC, id_documento DESC LIMIT 1"
        )
        r = self.cur.fetchone()
        return r["id_documento"] if r else None

    # articulos
    def insert_articulo(self, id_documento: int, numero: Optional[str], texto: Optional[str]) -> int:
        if DRY_RUN:
            print("[DRY RUN] insert_articulo", id_documento, numero); 
            return -1
        sql = (
            "INSERT INTO articulos (id_documento, numero_articulo, texto_articulo, embedding_articulo) "
            "VALUES (%s,%s,%s,NULL)"
        )
        self.cur.execute(sql, [id_documento, numero, texto])
        return self.cur.lastrowid

    # anexos
    def insert_anexo(self, id_documento: int, nombre_anexo: str, texto_anexo: str) -> int:
        if DRY_RUN:
            print("[DRY RUN] insert_anexo", id_documento, nombre_anexo); 
            return -1
        sql = (
            "INSERT INTO anexos (id_documento, nombre_anexo, texto_anexo, ruta_archivo, embedding_completo, embedding_texto) "
            "VALUES (%s,%s,%s,%s,NULL,NULL)"
        )
        self.cur.execute(sql, [id_documento, nombre_anexo, texto_anexo, None])
        return self.cur.lastrowid

    # modificaciones (v0.5.1)
    def insert_modificacion(self, id_documento: int,
                            nombre_regulacion: Optional[str],
                            texto_mod: Optional[str],
                            fecha_pub: Optional[date]) -> int:
        if DRY_RUN:
            print("[DRY RUN] insert_modificacion", id_documento, fecha_pub, "len(texto)=", len(texto_mod) if texto_mod else 0); 
            return -1
        sql = (
            "INSERT INTO modificaciones (id_documento, nombre_regulacion, texto_modificacion, fecha_publicacion, ruta_archivo, embedding_completo) "
            "VALUES (%s,%s,%s,%s,%s,%s)"
        )
        self.cur.execute(sql, [id_documento, nombre_regulacion, texto_mod, fecha_pub, None, None])
        return self.cur.lastrowid

# ==========================
# PROCESO
# ==========================
@dataclass
class ResultadoDocumento:
    id_documento: int
    titulo: str
    fecha: Optional[date]
    tipo: str
    n_articulos: int

def procesar_pdf_compulsado(path_pdf: str, ambito: str = "Federal", emisor: str = "Comisión Nacional Bancaria y de Valores") -> ResultadoDocumento:
    t1, full = texto_pdf(path_pdf)
    titulo = detectar_nombre_regulacion_documento(t1, full) or "Sin título detectado"
    tipo = tipo_desde_nombre(titulo)
    fecha = fecha_publicacion_mas_reciente(full)

    db = DB(DB_CONFIG)
    try:
        id_doc = db.insert_documento(titulo, ambito, tipo, fecha, emisor)
        arts = partir_articulos(full)  # incluye Transitorios
        for num, cuerpo in arts:
            db.insert_articulo(id_doc, num, cuerpo)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return ResultadoDocumento(id_documento=id_doc, titulo=titulo, fecha=fecha, tipo=tipo, n_articulos=len(arts))

def procesar_pdf_modificacion(path_pdf: str) -> Optional[int]:
    """
    v0.5.1: Inserta UNA fila en `modificaciones` por PDF.
      - nombre_regulacion: SOLO 1ª hoja y SOLO patrones dados (incluye 'Resolución modificatoria', 'Resolución que modifica').
      - texto_modificacion: TODO el texto del PDF.
      - fecha_publicacion: detectada en texto; si no, usa AAAAMMDD del archivo.
      - ruta_archivo, embedding_completo: NULL.
    """
    fname = os.path.basename(path_pdf)
    fecha_archivo = parse_fecha_from_filename(fname)
    t1, full = texto_pdf(path_pdf)

    nombre_reg = detectar_nombre_norma_por_patron_modificaciones(t1) or os.path.splitext(fname)[0]
    fecha_texto = fecha_publicacion_mas_reciente(full)
    fecha_final = fecha_texto or fecha_archivo

    if not fecha_archivo:
        print(f"[WARN] No se encontró fecha AAAAMMDD en nombre: {fname}. Se buscará por fecha del texto ({fecha_texto}).")

    db = DB(DB_CONFIG)
    try:
        id_doc = None
        if fecha_archivo:
            id_doc = db.find_documento_by_fecha(fecha_archivo)
        if not id_doc and fecha_texto:
            id_doc = db.find_documento_by_fecha(fecha_texto)

        if not id_doc:
            print(f"[WARN] No existe documento con fecha_publicacion = {fecha_archivo or fecha_texto} para {fname}. Se omite.")
            db.close()
            return None

        db.insert_modificacion(id_doc, nombre_reg, full, fecha_final)
        db.commit()
        print(f"[OK] Modificación insertada para documento {id_doc} desde {fname}")
        return id_doc
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def procesar_pdf_anexo(path_pdf: str) -> Optional[int]:
    fname = os.path.basename(path_pdf)
    t1, full = texto_pdf(path_pdf)
    nombre_anexo = detectar_nombre_regulacion_documento(t1, full) or os.path.splitext(fname)[0]

    db = DB(DB_CONFIG)
    try:
        id_doc = db.find_documento_mas_reciente_por_normativa("")
        if not id_doc:
            print(f"[WARN] No hay documentos para vincular anexo: {fname}. Se omite.")
            return None
        db.insert_anexo(id_doc, nombre_anexo, full)
        db.commit()
        print(f"[OK] Anexo insertado y vinculado a documento {id_doc} desde {fname}")
        return id_doc
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

# ==========================
# SCAN CARPETAS
# ==========================
def scan_dir(path: str) -> List[str]:
    if not os.path.isdir(path):
        print(f"[WARN] Carpeta no encontrada: {path}")
        return []
    pdfs = []
    for root, _, files in os.walk(path):
        for f in files:
            if f.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, f))
    pdfs.sort()
    return pdfs

def scan_compulsadas() -> None:
    print(f"[SCAN] Compulsadas: {DIR_COMPULSADAS}")
    for pdf in scan_dir(DIR_COMPULSADAS):
        try:
            res = procesar_pdf_compulsado(pdf)
            print(f"[OK] Documento id={res.id_documento} | {res.titulo} | fecha={res.fecha} | artículos={res.n_articulos}")
        except Exception as e:
            print(f"[ERROR] {pdf}: {e}")

def scan_modificaciones() -> None:
    print(f"[SCAN] Modificaciones: {DIR_MODIFICACIONES}")
    for pdf in scan_dir(DIR_MODIFICACIONES):
        try:
            procesar_pdf_modificacion(pdf)
        except Exception as e:
            print(f"[ERROR] {pdf}: {e}")

def scan_anexos() -> None:
    print(f"[SCAN] Anexos: {DIR_ANEXOS}")
    for pdf in scan_dir(DIR_ANEXOS):
        try:
            procesar_pdf_anexo(pdf)
        except Exception as e:
            print(f"[ERROR] {pdf}: {e}")

# ==========================
# CLI
# ==========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extractor CNBV v0.5.1 (carpetas completas)")
    parser.add_argument("--scan-all", action="store_true", help="Procesa Compulsadas, luego Modificaciones y Anexos")
    parser.add_argument("--scan-compulsadas", action="store_true", help="Procesa solo 'Versiones compulsadas'")
    parser.add_argument("--scan-modificaciones", action="store_true", help="Procesa solo 'Modificaciones'")
    parser.add_argument("--scan-anexos", action="store_true", help="Procesa solo 'Anexos vigentes'")
    parser.add_argument("--ambito", default="Federal")
    parser.add_argument("--emisor", default="Comisión Nacional Bancaria y de Valores")
    parser.add_argument("--dry-run", action="store_true", help="No escribe en la base; solo muestra en consola")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        print("[MODO] DRY RUN activo (no se escribe en la base).")

    if args.scan_all:
        scan_compulsadas()
        scan_modificaciones()
        scan_anexos()
    else:
        done = False
        if args.scan_compulsadas:
            scan_compulsadas(); done = True
        if args.scan_modificaciones:
            scan_modificaciones(); done = True
        if args.scan_anexos:
            scan_anexos(); done = True
        if not done:
            parser.print_help()
