
import pandas as pd
import numpy as np

# Load the policy data from the same dbt seeds source as the sql version
policies = pd.read_csv(
    "seeds/policy_data.csv",
    parse_dates=["effective_date", "expiration_date"]
)

# Calculate the total number of days the policy is in effect
policies["policy_days"] = (policies["expiration_date"] - policies["effective_date"]).dt.days

# Manually build a monthly calendar for 2020. We could reference months.sql, but that would require connecting to Snowflake
month_starts = pd.date_range("2020-01-01", "2020-12-01", freq="MS")
month_ends = month_starts + pd.offsets.MonthEnd(0)
calendar = pd.DataFrame({"month_start": month_starts, "month_end": month_ends})

# Loop through every policy and every month
# Check if the policy was active during that month, and if so, calculate earned days
rows = []
for policy_index, policy in policies.iterrows():
    for month_index, month in calendar.iterrows():
        # Check if the policy overlaps with the month
        if policy["expiration_date"] >= month["month_start"] and policy["effective_date"] <= month["month_end"]: # use same logic as we did in the dbt int table
            in_force_start = max(policy["effective_date"], month["month_start"]) 
            in_force_end = min(policy["expiration_date"], month["month_end"])
            earned_days = (in_force_end - in_force_start).days + 1

            rows.append({
                "policy_number": policy["policy_number"],
                "written_premium": policy["written_premium"],
                "policy_days": policy["policy_days"],
                "month_start": month["month_start"],
                "earned_days": earned_days
            })

# Convert the list of rows into a DataFrame
policy_months = pd.DataFrame(rows)

# Define a simple function to calculate monthly earned premium
def calc_monthly(df):
    df = df.sort_values("month_start").copy()

    # Calculate cumulative days active for the policy
    df["cumulative_days"] = df["earned_days"].cumsum()

    # All rows in the group have the same written_premium, so just grab the first one
    written_premium = df["written_premium"].iloc[0]

    # Calculate cumulative earned premium before rounding
    cumulative_unrounded = written_premium * df["cumulative_days"] / df["policy_days"]

    # Round cumulative premium to 2 decimal places
    df["cumulative_earned_rounded"] = cumulative_unrounded.round(2)

    # Calculate monthly earned premium by subtracting last months total from this months
    monthly_diff = df["cumulative_earned_rounded"].diff()
    # For the first month, theres no previous value, so we use the cumulative as is
    df["earned_premium"] = monthly_diff.fillna(df["cumulative_earned_rounded"])

    return df[["policy_number", "month_start", "earned_premium"]]

# Apply the calculation for each policy
monthly = policy_months.groupby("policy_number", group_keys=False).apply(calc_monthly)

# Format month for readability
monthly["earn_month"] = monthly["month_start"].dt.strftime("%Y%m")
monthly_output = monthly[["policy_number", "earn_month", "earned_premium"]]

# Calculate total earned for the year
annual_output = (
    monthly.groupby("policy_number", as_index=False)["earned_premium"]
    .sum()
    .assign(earn_year="2020") # restricting to 2020 as per instruction
    [["policy_number", "earn_year", "earned_premium"]]
)

if __name__ == "__main__":
    pd.set_option("display.float_format", "{:.2f}".format)
    pd.set_option("display.max_rows", None)

    print("\nFULL MONTHLY RESULTS:\n")
    print(monthly_output)

    print("ANNUAL TOTALS:\n")
    print(annual_output)