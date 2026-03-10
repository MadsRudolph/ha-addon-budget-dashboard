"""
PDF report generator for the Budget Dashboard.
Uses fpdf2 to produce a styled monthly budget report with KPIs, tables,
and budget-vs-actual analysis. No image/chart dependencies.
"""

from datetime import datetime
from fpdf import FPDF
import pandas as pd


def fmt_dkk(value):
    """Format a number as DKK with thousands separator and no decimals."""
    if pd.isna(value):
        return "0 DKK"
    sign = "-" if value < 0 else ""
    formatted = f"{abs(value):,.0f}".replace(",", ".")
    return f"{sign}{formatted} DKK"


def fmt_pct(value):
    """Format a number as a percentage string."""
    if pd.isna(value):
        return "0.0%"
    return f"{value:.1f}%"


# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
BLUE = (41, 98, 255)
BLUE_LIGHT = (232, 240, 254)
DARK = (30, 30, 30)
GREY_TEXT = (100, 100, 100)
WHITE = (255, 255, 255)
GREEN = (16, 163, 74)
RED = (220, 53, 69)
ROW_ALT = (245, 247, 250)
CARD_BG = (243, 246, 252)
DIVIDER = (200, 210, 230)


class BudgetReport(FPDF):
    """Custom FPDF subclass for the monthly budget report."""

    def __init__(self, report_month_label: str = ""):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.report_month_label = report_month_label
        self.set_auto_page_break(auto=True, margin=20)

    # ------------------------------------------------------------------
    # Header / Footer
    # ------------------------------------------------------------------
    def header(self):
        # Title
        self.set_font("Helvetica", "B", 20)
        self.set_text_color(*DARK)
        self.cell(0, 12, "Monthly Budget Report", new_x="LMARGIN", new_y="NEXT", align="C")

        # Subtitle: month label + generation timestamp
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*GREY_TEXT)
        subtitle_parts = []
        if self.report_month_label:
            subtitle_parts.append(self.report_month_label)
        subtitle_parts.append(f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        self.cell(0, 6, "  |  ".join(subtitle_parts), new_x="LMARGIN", new_y="NEXT", align="C")

        # Divider line
        self.ln(2)
        y = self.get_y()
        self.set_draw_color(*DIVIDER)
        self.set_line_width(0.5)
        self.line(10, y, self.w - 10, y)
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*GREY_TEXT)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    # ------------------------------------------------------------------
    # KPI cards
    # ------------------------------------------------------------------
    def add_kpi_row(self, kpis):
        """
        Render a row of KPI cards.

        Parameters
        ----------
        kpis : list[tuple[str, str]]
            Each tuple is (label, formatted_value).
        """
        if not kpis:
            return

        usable_w = self.w - 20  # 10 mm margin each side
        gap = 3
        n = len(kpis)
        card_w = (usable_w - gap * (n - 1)) / n
        card_h = 22
        start_x = 10
        start_y = self.get_y()

        for i, (label, value) in enumerate(kpis):
            x = start_x + i * (card_w + gap)

            # Card background
            self.set_fill_color(*CARD_BG)
            self.rect(x, start_y, card_w, card_h, style="F")

            # Thin top accent line
            self.set_draw_color(*BLUE)
            self.set_line_width(0.8)
            self.line(x, start_y, x + card_w, start_y)

            # Value
            self.set_xy(x, start_y + 3)
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(*DARK)
            self.cell(card_w, 7, value, align="C")

            # Label
            self.set_xy(x, start_y + 11)
            self.set_font("Helvetica", "", 8)
            self.set_text_color(*GREY_TEXT)
            self.cell(card_w, 6, label, align="C")

        self.set_y(start_y + card_h + 6)

    # ------------------------------------------------------------------
    # Section title
    # ------------------------------------------------------------------
    def add_section_title(self, title: str):
        """Render a blue section header with a small accent bar."""
        self.ln(2)

        # Check if we need a new page (at least 30 mm remaining)
        if self.get_y() > self.h - 40:
            self.add_page()

        y = self.get_y()

        # Accent bar
        self.set_fill_color(*BLUE)
        self.rect(10, y, 3, 8, style="F")

        # Title text
        self.set_xy(16, y)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*BLUE)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")

        self.set_text_color(*DARK)
        self.ln(3)

    # ------------------------------------------------------------------
    # Styled table
    # ------------------------------------------------------------------
    def add_table(self, headers, rows, col_widths=None):
        """
        Render a styled table with a blue header row and alternating body
        row colours.

        Parameters
        ----------
        headers : list[str]
        rows : list[list[str]]
        col_widths : list[float] | None
            If None, columns share the available width equally.
        """
        usable_w = self.w - 20
        if col_widths is None:
            col_widths = [usable_w / len(headers)] * len(headers)

        row_h = 7

        # --- Header row ---
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(*BLUE)
        self.set_text_color(*WHITE)
        x_start = 10
        for i, h in enumerate(headers):
            align = "L" if i == 0 else "R"
            self.set_x(x_start + sum(col_widths[:i]))
            self.cell(col_widths[i], row_h, f" {h}", fill=True, align=align)
        self.ln(row_h)

        # --- Body rows ---
        self.set_font("Helvetica", "", 9)
        for row_idx, row in enumerate(rows):
            # Page break check
            if self.get_y() + row_h > self.h - 20:
                self.add_page()
                # Re-draw header on new page
                self.set_font("Helvetica", "B", 9)
                self.set_fill_color(*BLUE)
                self.set_text_color(*WHITE)
                for i, h in enumerate(headers):
                    align = "L" if i == 0 else "R"
                    self.set_x(x_start + sum(col_widths[:i]))
                    self.cell(col_widths[i], row_h, f" {h}", fill=True, align=align)
                self.ln(row_h)
                self.set_font("Helvetica", "", 9)

            # Alternating background
            if row_idx % 2 == 1:
                self.set_fill_color(*ROW_ALT)
                fill = True
            else:
                self.set_fill_color(*WHITE)
                fill = True

            self.set_text_color(*DARK)

            for i, cell_val in enumerate(row):
                align = "L" if i == 0 else "R"
                self.set_x(x_start + sum(col_widths[:i]))

                # Colour OVER budget status red
                if cell_val == "OVER":
                    self.set_text_color(*RED)
                elif cell_val == "Under":
                    self.set_text_color(*GREEN)

                self.cell(col_widths[i], row_h, f" {cell_val} ", fill=fill, align=align)

                # Reset text colour
                self.set_text_color(*DARK)

            self.ln(row_h)

        self.ln(4)


# ======================================================================
# Public API
# ======================================================================

def generate_monthly_report(df: pd.DataFrame, budgets_df: pd.DataFrame, month: str) -> bytes:
    """
    Generate a styled PDF budget report for a single month.

    Parameters
    ----------
    df : pd.DataFrame
        Transaction data with columns:
        date, category, subcategory, description, amount, balance, month
    budgets_df : pd.DataFrame
        Budget targets with columns: category, monthly_limit
    month : str
        Month identifier (e.g. "2026-01") used to filter *df*.

    Returns
    -------
    bytes
        The finished PDF document as raw bytes.
    """
    # ------------------------------------------------------------------
    # Filter to selected month
    # ------------------------------------------------------------------
    mdf = df[df["month"] == month].copy()

    income = mdf.loc[mdf["amount"] > 0, "amount"].sum()
    expenses = mdf.loc[mdf["amount"] < 0, "amount"].sum()
    net = income + expenses
    tx_count = len(mdf)

    # Pretty month label
    try:
        month_dt = datetime.strptime(month, "%Y-%m")
        month_label = month_dt.strftime("%B %Y")
    except ValueError:
        month_label = month

    # ------------------------------------------------------------------
    # Build PDF
    # ------------------------------------------------------------------
    pdf = BudgetReport(report_month_label=month_label)
    pdf.alias_nb_pages()
    pdf.add_page()

    # --- KPIs ---
    pdf.add_section_title("Key Performance Indicators")
    pdf.add_kpi_row([
        ("Total Income", fmt_dkk(income)),
        ("Total Expenses", fmt_dkk(expenses)),
        ("Net Cash Flow", fmt_dkk(net)),
        ("Transactions", str(tx_count)),
    ])

    # ------------------------------------------------------------------
    # Spending by Category
    # ------------------------------------------------------------------
    pdf.add_section_title("Spending by Category")

    expense_df = mdf[mdf["amount"] < 0].copy()
    if not expense_df.empty:
        cat_spend = (
            expense_df
            .groupby("category")["amount"]
            .sum()
            .abs()
            .sort_values(ascending=False)
        )
        total_spend = cat_spend.sum()
        cat_rows = []
        for cat, amt in cat_spend.items():
            pct = (amt / total_spend * 100) if total_spend else 0
            cat_rows.append([str(cat), fmt_dkk(amt), fmt_pct(pct)])

        # Totals row
        cat_rows.append(["TOTAL", fmt_dkk(total_spend), "100.0%"])

        usable = pdf.w - 20
        pdf.add_table(
            headers=["Category", "Amount", "% of Total"],
            rows=cat_rows,
            col_widths=[usable * 0.50, usable * 0.28, usable * 0.22],
        )
    else:
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(*GREY_TEXT)
        pdf.cell(0, 8, "No expenses recorded for this month.", new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------------------------
    # Budget vs Actual
    # ------------------------------------------------------------------
    pdf.add_section_title("Budget vs Actual")

    if budgets_df is not None and not budgets_df.empty and not expense_df.empty:
        cat_spent = (
            expense_df
            .groupby("category")["amount"]
            .sum()
            .abs()
        )
        bva_rows = []
        for _, brow in budgets_df.iterrows():
            cat = brow["category"]
            limit = float(brow["monthly_limit"])
            spent = float(cat_spent.get(cat, 0))
            diff = limit - spent
            status = "OVER" if diff < 0 else "Under"
            bva_rows.append([
                str(cat),
                fmt_dkk(spent),
                fmt_dkk(limit),
                fmt_dkk(diff),
                status,
            ])

        usable = pdf.w - 20
        pdf.add_table(
            headers=["Category", "Spent", "Budget", "Difference", "Status"],
            rows=bva_rows,
            col_widths=[
                usable * 0.28,
                usable * 0.20,
                usable * 0.20,
                usable * 0.20,
                usable * 0.12,
            ],
        )
    else:
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(*GREY_TEXT)
        pdf.cell(0, 8, "No budget targets configured.", new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------------------------
    # Top 10 Merchants
    # ------------------------------------------------------------------
    pdf.add_section_title("Top 10 Merchants")

    if not expense_df.empty:
        merchants = (
            expense_df
            .groupby("description")
            .agg(total=("amount", lambda x: x.abs().sum()), count=("amount", "count"))
            .sort_values("total", ascending=False)
            .head(10)
        )
        merch_rows = []
        for desc, row in merchants.iterrows():
            merch_rows.append([
                str(desc)[:40],
                fmt_dkk(row["total"]),
                str(int(row["count"])),
            ])

        usable = pdf.w - 20
        pdf.add_table(
            headers=["Merchant", "Total Spent", "# Transactions"],
            rows=merch_rows,
            col_widths=[usable * 0.50, usable * 0.28, usable * 0.22],
        )
    else:
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(*GREY_TEXT)
        pdf.cell(0, 8, "No merchant data for this month.", new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------------------------
    # Output as bytes
    # ------------------------------------------------------------------
    pdf_bytes = pdf.output()
    if isinstance(pdf_bytes, bytearray):
        pdf_bytes = bytes(pdf_bytes)
    return pdf_bytes
