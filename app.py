from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

DEFAULT_STATUS = "Ожидает отгрузки"
SHEET_NAME = "Основной список"

app = FastAPI()

PAGE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Ozon → Основной список</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;max-width:760px;margin:40px auto;padding:0 16px}
    .card{border:1px solid #ddd;border-radius:16px;padding:18px}
    input,button{font-size:16px}
    button{padding:10px 14px;border-radius:12px;border:0;background:#111;color:#fff;cursor:pointer}
    .muted{color:#666;font-size:14px;line-height:1.4}
    .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
    .pill{display:inline-block;padding:4px 10px;border-radius:999px;background:#f3f3f3;font-size:13px}
  </style>
</head>
<body>
  <h1>Ozon CSV → «Основной список» (Excel под печать)</h1>
  <div class="card">
    <div class="row">
      <span class="pill">Статус: Ожидает отгрузки</span>
      <span class="pill">Лист: Основной список</span>
    </div>
    <p class="muted">
      Группирует по <b>Артикулу</b>, суммирует количество, добавляет пометки вида <b>8(2в одну, 3в одну)</b>
      только если в исходных строках встречались количества &gt; 1.
      Внизу добавляет только те отправления, где в одной отправке несколько артикулов (без номеров).
    </p>
    <form action="/convert" method="post" enctype="multipart/form-data">
      <p><input type="file" name="file" accept=".csv" required></p>
      <p><button type="submit">Скачать Excel</button></p>
    </form>
  </div>
</body>
</html>
"""

def read_csv_smart(content: bytes) -> pd.DataFrame:
    for enc in ("utf-8", "cp1251"):
        try:
            return pd.read_csv(BytesIO(content), sep=";", encoding=enc, engine="python")
        except Exception:
            continue
    return pd.read_csv(BytesIO(content), sep=";", engine="python")

def build_note(qtys: pd.Series) -> str:
    bigger = sorted({int(q) for q in qtys.dropna() if int(q) > 1})
    if not bigger:
        return ""
    return ", ".join([f"{q}в одну" for q in bigger])

def build_main_df(df_ready: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df_ready.groupby(["Артикул"], as_index=False)
        .agg(Итого=("Количество","sum"), Пометка=("Количество", build_note))
    )
    grouped["Количество"] = grouped.apply(
        lambda r: f"{int(r['Итого'])}({r['Пометка']})" if r["Пометка"] else f"{int(r['Итого'])}",
        axis=1
    )
    top_df = pd.DataFrame({
        "Артикул": grouped["Артикул"],
        "Количество": grouped["Количество"],
        "КОД": ["" for _ in range(len(grouped))]
    })

    blocks = []
    for ship, ship_df in df_ready.groupby("Номер отправления"):
        if ship_df["Артикул"].nunique() <= 1:
            continue
        items = (
            ship_df.groupby("Артикул")["Количество"]
            .sum()
            .reset_index()
            .sort_values("Артикул")
        )
        line = "; ".join([f"{r['Артикул']} — {int(r['Количество'])}" for _, r in items.iterrows()])
        blocks.append({"Артикул": line, "Количество": "", "КОД": ""})

    bottom_df = pd.DataFrame(blocks)
    spacer = pd.DataFrame([{"Артикул":"", "Количество":"", "КОД":""} for _ in range(2)])
    return pd.concat([top_df, spacer, bottom_df], ignore_index=True)

def format_as_osnovnoi_spisok(xlsx_bytes: bytes) -> bytes:
    bio = BytesIO(xlsx_bytes)
    wb = load_workbook(bio)
    ws = wb.active
    ws.title = SHEET_NAME

    header_font = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=False)
    left = Alignment(horizontal="left", vertical="center", wrap_text=False)
    thin = Side(style="thin")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for cell in ws[1]:
        cell.font = header_font
        cell.alignment = center
        cell.border = border

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.border = border
            if cell.column == 2:
                cell.alignment = center
            else:
                cell.alignment = left

    widths = {1: 30, 2: 18, 3: 15}
    for col, w in widths.items():
        ws.column_dimensions[get_column_letter(col)].width = w

    for r in range(2, ws.max_row + 1):
        a = ws.cell(row=r, column=1).value
        b = ws.cell(row=r, column=2).value
        c = ws.cell(row=r, column=3).value
        if (isinstance(a, str) and a.strip() and (not b) and (not c) and ("—" in a or ";" in a)):
            ws.cell(row=r, column=1).font = Font(bold=True)

    ws.page_setup.orientation = ws.ORIENTATION_PORTRAIT
    ws.page_setup.fitToHeight = 1
    ws.page_setup.fitToWidth = 1
    ws.page_margins.left = 0.5
    ws.page_margins.right = 0.5
    ws.page_margins.top = 0.75
    ws.page_margins.bottom = 0.75

    out = BytesIO()
    wb.save(out)
    return out.getvalue()

@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE

@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    content = await file.read()
    df = read_csv_smart(content)

    required = {"Статус", "Артикул", "Количество", "Номер отправления"}
    missing = required - set(df.columns)
    if missing:
        msg = "В CSV не хватает колонок: " + ", ".join(sorted(missing))
        return HTMLResponse(f"<h3>{msg}</h3>", status_code=400)

    df_ready = df[df["Статус"] == DEFAULT_STATUS].copy()
    df_ready["Количество"] = pd.to_numeric(df_ready["Количество"], errors="coerce").fillna(0).astype(int)

    final_df = build_main_df(df_ready)

    xlsx_io = BytesIO()
    final_df.to_excel(xlsx_io, index=False)
    formatted = format_as_osnovnoi_spisok(xlsx_io.getvalue())

    filename = "Основной_список.xlsx"
    return StreamingResponse(
        BytesIO(formatted),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
