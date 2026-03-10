"""
Claude API integration for financial insights.

Uses the Anthropic Python SDK to generate personalised financial analysis
from pre-aggregated transaction data.  Designed to keep token usage low by
sending a compact JSON summary rather than raw transactions.

Requires:
    pip install anthropic
    Environment variable ANTHROPIC_API_KEY must be set.
"""

import json
from datetime import datetime

import anthropic
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a concise, friendly financial advisor for Mads, a Danish university \
student at DTU (semester 4 of 7, ~3 semesters remaining).

Key context:
- Income: SU grant ~6,500 DKK/mo, SU loan ~3,625 DKK/mo (goal: eliminate the loan)
- Job: waiter at Hotel D'Angleterre, 185 DKK/h, 50% overtime after 8 h, typical 12 h shifts
- Snus habit: ~65 DKK/day plus gas-station drinks — wants to quit (saves ~2,700 DKK/mo)
- Insight: quitting snus + picking up 1 extra shift/mo ≈ enough to drop the SU loan entirely
- SU loan rate while studying: 4% p.a. fixed, compounded monthly
- All amounts are in Danish kroner (DKK)

Guidelines:
- Be specific: reference actual numbers from the data provided.
- Highlight wins (under-budget categories, income growth) as well as risks.
- When relevant, tie advice back to the SU-loan-elimination goal.
- Keep responses short (≤ 300 words) unless the user asks for detail.
- Use DKK as the currency symbol (not kr or kr.).
"""

# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def prepare_financial_summary(
    df: pd.DataFrame,
    budgets_df: pd.DataFrame | None = None,
) -> dict:
    """Pre-aggregate transaction data into a compact dict for the LLM.

    Parameters
    ----------
    df : pd.DataFrame
        Transactions with columns: date, category, subcategory, description,
        amount (negative = expense), balance, month (str "YYYY-MM").
    budgets_df : pd.DataFrame | None
        Optional budget targets with columns: category, budget.

    Returns
    -------
    dict
        JSON-serialisable summary suitable for the Claude prompt.
    """

    summary: dict = {}

    # --- current month ---
    now_month = datetime.now().strftime("%Y-%m")
    summary["generated_at"] = datetime.now().isoformat(timespec="seconds")
    summary["current_month"] = now_month

    # --- monthly spending by category (expenses only) ---
    expenses = df[df["amount"] < 0].copy()
    expenses["abs_amount"] = expenses["amount"].abs()

    monthly_cat = (
        expenses.groupby(["month", "category"])["abs_amount"]
        .sum()
        .round(2)
        .reset_index()
    )
    # Pivot: {month: {category: total, …}, …}
    spending_by_month: dict = {}
    for _, row in monthly_cat.iterrows():
        m = row["month"]
        spending_by_month.setdefault(m, {})[row["category"]] = row["abs_amount"]
    summary["monthly_spending_by_category"] = spending_by_month

    # --- monthly income ---
    income = df[df["amount"] > 0].copy()
    monthly_income = (
        income.groupby("month")["amount"].sum().round(2).to_dict()
    )
    summary["monthly_income"] = monthly_income

    # --- top merchants (by total absolute spend) ---
    top_merchants = (
        expenses.groupby("description")["abs_amount"]
        .sum()
        .nlargest(15)
        .round(2)
        .to_dict()
    )
    summary["top_merchants"] = top_merchants

    # --- current-month snapshot ---
    cm_expenses = expenses[expenses["month"] == now_month]
    cm_income = income[income["month"] == now_month]
    summary["current_month_status"] = {
        "total_spending": round(cm_expenses["abs_amount"].sum(), 2),
        "total_income": round(cm_income["amount"].sum(), 2),
        "transaction_count": len(df[df["month"] == now_month]),
        "top_categories": (
            cm_expenses.groupby("category")["abs_amount"]
            .sum()
            .nlargest(5)
            .round(2)
            .to_dict()
        ),
    }

    # --- latest balance ---
    if not df.empty and "balance" in df.columns:
        latest = df.sort_values("date").iloc[-1]
        summary["latest_balance"] = round(float(latest["balance"]), 2)
        summary["latest_balance_date"] = str(latest["date"])[:10]

    # --- budget targets ---
    if budgets_df is not None and not budgets_df.empty:
        summary["budget_targets"] = dict(
            zip(budgets_df["category"], budgets_df["budget"].round(2))
        )

        # budget vs actual for the current month
        if now_month in spending_by_month:
            actual = spending_by_month[now_month]
            bva: dict = {}
            for _, brow in budgets_df.iterrows():
                cat = brow["category"]
                budget_val = round(brow["budget"], 2)
                spent = actual.get(cat, 0.0)
                bva[cat] = {
                    "budget": budget_val,
                    "spent": spent,
                    "remaining": round(budget_val - spent, 2),
                }
            summary["budget_vs_actual"] = bva

    return summary


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------


def get_financial_insights(
    df: pd.DataFrame,
    budgets_df: pd.DataFrame | None = None,
    question: str | None = None,
    model: str = "claude-haiku-4-5-20251001",
) -> str:
    """Generate financial insights via the Claude API.

    Parameters
    ----------
    df : pd.DataFrame
        Transaction dataframe (same schema as ``prepare_financial_summary``).
    budgets_df : pd.DataFrame | None
        Optional budget targets.
    question : str | None
        A specific question to ask.  If *None*, a general monthly analysis is
        returned.
    model : str
        Anthropic model ID.  Defaults to Claude Haiku for low cost / latency.

    Returns
    -------
    str
        The assistant's text response, or an error message string if something
        goes wrong.
    """

    try:
        client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    except anthropic.AuthenticationError:
        return (
            "Error: ANTHROPIC_API_KEY is not set or invalid. "
            "Please add it in Settings → AI Insights."
        )
    except Exception as exc:
        return f"Error initialising Anthropic client: {exc}"

    # Build the compact summary
    try:
        summary = prepare_financial_summary(df, budgets_df)
        summary_json = json.dumps(summary, ensure_ascii=False, default=str)
    except Exception as exc:
        return f"Error preparing financial summary: {exc}"

    # Compose the user message
    if question:
        user_message = (
            f"Here is my financial data:\n\n```json\n{summary_json}\n```\n\n"
            f"Question: {question}"
        )
    else:
        user_message = (
            f"Here is my financial data:\n\n```json\n{summary_json}\n```\n\n"
            "Give me a brief monthly financial check-in.  Highlight what went "
            "well, what needs attention, and one concrete action I can take "
            "this week to move closer to eliminating my SU loan."
        )

    # Call the API
    try:
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text
    except anthropic.AuthenticationError:
        return (
            "Error: Invalid API key. Please check ANTHROPIC_API_KEY in your "
            "environment variables."
        )
    except anthropic.RateLimitError:
        return (
            "Error: API rate limit reached. Please wait a moment and try again."
        )
    except anthropic.APIConnectionError:
        return (
            "Error: Could not connect to the Anthropic API. "
            "Check your internet connection."
        )
    except anthropic.APIStatusError as exc:
        return f"Error: Anthropic API returned status {exc.status_code}: {exc.message}"
    except Exception as exc:
        return f"Error generating insights: {exc}"
