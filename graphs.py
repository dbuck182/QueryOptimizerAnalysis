import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

def plot_join_error_boxplots(param_df):
    # Compute signed log error
    df = param_df.copy()
    df["error"] = np.log10(df["Plan Rows"] + 1) - np.log10(df["Actual Rows"] + 1)

    # Need to drop the non joins
    df = df.dropna(subset=["Join Index"])

    df["Join Index"] = df["Join Index"].astype(int)
    df = df.sort_values("Join Index")

    # Group data by join index
    grouped = [df[df["Join Index"] == j]["error"] for j in sorted(df["Join Index"].unique())]
    labels = sorted(df["Join Index"].unique())

    # Plot
    plt.figure(figsize=(12, 6))
    plt.boxplot(grouped, labels=labels, showfliers=True)
    plt.xscale("log")
    plt.yscale("log")
    plt.axhline(0, color="black", linestyle="--", linewidth=1)
    plt.xlabel("Join Index")
    plt.ylabel("Signed Log Error (log10(actual) - log10(estimate))")
    plt.title("TCPH: Cardinality Estimation Error by Join Index")
    plt.grid(axis="y")
    plt.show()

def plot_type_of_join(param_df):
    df = param_df.copy()

    df["error"] = np.log10(df["Plan Rows"] + 1) - np.log10(df["Actual Rows"] + 1)

    # Need to drop the non joins
    df = df.dropna(subset=["Join Index"])
    
    df.boxplot(column="error", by="Node Type", figsize=(10,6))
    plt.axhline(0, color="black", linestyle="--")
    plt.title("Job: Error by Join Type")
    plt.suptitle("")
    plt.ylabel("Signed Log Error")
    plt.show()


def plot_cost_vs_time(df):
    plot_df = df[(df["Actual Total Time"] > 0) & (df["Total Cost"] > 0)]

    plt.figure(figsize=(10, 6))
    plt.scatter(
        plot_df["Total Cost"],
        plot_df["Actual Total Time"],
        alpha=0.5,
        s=20
    )

    plt.xscale("log")
    plt.yscale("log")

    plt.xlabel("Cost (log scale)")
    plt.ylabel("Actual Runtime (ms) (log scale)")
    plt.title("Job: Estimated Cost vs Actual Runtime")

    plt.grid(True, which="both", linestyle="--")
    plt.show()

def q_error_table(param_df):
    # Need to drop the non joins
    df = param_df.copy()
    df = df.dropna(subset=["Join Index"])
    df["q_error"] = df.apply(
    lambda r: max(
        (r["Actual Rows"] + 1) / (r["Plan Rows"] + 1),
        (r["Plan Rows"] + 1) / (r["Actual Rows"] + 1)
    ),
    axis=1
    )

    bins = [1, 2, 10, 100, float("inf")]
    labels = ["1–2×", "2–10×", "10–100×", ">100×"]

    df["q_bucket"] = pd.cut(df["q_error"], bins=bins, labels=labels)


    join_error_runtime = df.groupby("q_bucket").agg(
    mean_runtime=("Actual Total Time", "mean"),
    median_runtime=("Actual Total Time", "median"),
    count=("Actual Total Time", "count")
    )
    
    print(tabulate(join_error_runtime, headers="keys", tablefmt="latex"))




stack1_df = pd.read_csv("stack1.csv")
stack2_df = pd.read_csv("stack2.csv")
# Unfortunately was unable to get the queries from stack_3 to run
#stack3_df = pd.read_csv("stack1.csv")
stack4_df = pd.read_csv("stack4.csv")
stack5_df = pd.read_csv("stack5.csv")
stack6_df = pd.read_csv("stack6.csv")
stack7_df = pd.read_csv("stack7.csv")
stack8_df = pd.read_csv("stack8.csv")
stack9_df = pd.read_csv("stack9.csv")
stack10_df = pd.read_csv("stack10.csv")
stack11_df = pd.read_csv("stack11.csv")
stack12_df = pd.read_csv("stack12.csv")
stack13_df = pd.read_csv("stack13.csv")
stack14_df = pd.read_csv("stack14.csv")
stack15_df = pd.read_csv("stack15.csv")
stack16_df = pd.read_csv("stack16.csv")

stack_df = pd.concat([stack1_df, stack2_df, stack4_df, stack5_df, stack6_df, stack7_df, stack8_df, stack9_df, stack10_df, stack11_df, stack12_df, stack13_df, stack14_df,stack15_df, stack16_df ], ignore_index=True)

tcphmain_df = pd.read_csv("tcphtestsagain.csv")
tcph15_df = pd.read_csv("query15tcph.csv")
tcph_df = pd.concat([tcphmain_df, tcph15_df], ignore_index=True)


job_df = pd.read_csv("jobqueries.csv")

job_no_loop = pd.read_csv("noNestedLoopsJob.csv")
#plot_join_error_boxplots(job_df)
#plot_type_of_join(job_df)
#plot_join_error_boxplots(tcph_df)
#plot_cost_vs_time(job_df)
# plot_join_error_boxplots(job_no_loop)
#plot_join_error_boxplots(stack_df)
q_error_table(job_df)
