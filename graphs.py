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
    plt.axhline(0, color="black", linestyle="--", linewidth=1)
    plt.xlabel("Join Index")
    plt.ylabel("Signed Log Error (log10(actual) - log10(estimate))")
    plt.title("TPCH: Cardinality Estimation Error by Join Index")
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

def q_error_table(df1, df2, df3):
    # Need to drop the non joins
    df1 = df1.dropna(subset=["Join Index"])
    df2 = df2.dropna(subset=["Join Index"])
    df3 = df3.dropna(subset=["Join Index"])

    df1["Actual Rows"] = df1["Actual Rows"].replace(0, 1)
    df1["Plan Rows"] = df1["Plan Rows"].replace(0, 1)
    df2["Plan Rows"] = df2["Plan Rows"].replace(0, 1)
    df2["Actual Rows"] = df2["Actual Rows"].replace(0, 1)
    df3["Plan Rows"] = df3["Plan Rows"].replace(0, 1)
    df3["Actual Rows"] = df3["Actual Rows"].replace(0, 1)
    # Needed to add 1 to not have any divide by 0 errors
    df1["q_error"] = np.maximum(df1["Actual Rows"] / df1["Plan Rows"] , df1["Plan Rows"] / df1["Actual Rows"])

    df2["q_error"] = np.maximum(df2["Actual Rows"] / df2["Plan Rows"] , df2["Plan Rows"] / df2["Actual Rows"])

    df3["q_error"] = np.maximum(df3["Actual Rows"] / df3["Plan Rows"] , df3["Plan Rows"] / df3["Actual Rows"])
    print(df2[["Actual Rows", "Plan Rows", "q_error", "Query Name"]].head(20))

    summary = pd.DataFrame({
        "Job": [df1["q_error"].mean(), df1["q_error"].max(), df1["q_error"].median()],
        "Stack": [df2["q_error"].mean(), df2["q_error"].max(), df2["q_error"].median()],
        "TPC-H": [df3["q_error"].mean(), df3["q_error"].max(), df3["q_error"].median()],
    }, index=["Mean Q-error", "Max Q-error", "Median Q-error"])
    
    print(tabulate(summary, headers="keys", tablefmt="latex"))

# scatter plot for looking at performance
def scatter_join_times(csv_with, csv_without):

    df_with = pd.read_csv(csv_with)
    df_without = pd.read_csv(csv_without)


    df_with = df_with.dropna(subset=["Join Index"])
    df_without = df_without.dropna(subset=["Join Index"])
    print("Before:", len(df_with))
    df_with = df_with[df_with["Node Type"] != "Nested Loop"]
    print("After:", len(df_with))
    # print("Dropped:", len(df_with))


    df_with["NLJ"] = "With NLJ"
    df_without["NLJ"] = "Without NLJ"

    df_final = pd.concat([df_with, df_without], ignore_index=True)

    plt.figure(figsize=(12, 6))

    for label, group in df_final.groupby("NLJ"):
        plt.scatter(
            group["Join Index"],
            group["Actual Total Time"],
            alpha=0.6,
            label=label
        )
    
    plt.xlabel("Join Index")
    plt.ylabel("Join Runtime (ms)")
    plt.title("Join Runtime Scatter: With vs Without Nested Loop Joins (Job)")
    plt.legend()
    plt.grid(True, linestyle="--")
    plt.show()


# compare the size of the tables
# Using q-error
def table_size_graph(df):
    # want to do a scatter plot
    #df = pd.read_csv(df_name)
    df_before = len(df)
   # print("Before:", len(df))
    # df = df.dropna(subset=["Join Index", "Plan Rows", "Actual Rows"])
    df = df.dropna(subset=["Join Index"])
    #print("After:", len(df))
    #print("Dropped:", df_before - len(df))

    #df = df.dropna(subset=["Join Index"])
    df = df.dropna(subset=["Plan Rows"])
    df = df.dropna(subset=["Actual Rows"])
    df["Actual Rows"] = df["Actual Rows"].replace(0, 1)
    df["Plan Rows"] = df["Plan Rows"].replace(0, 1)

    # Add error
    # Needed to add 1 to not have any divide by 0 errors
    df["q_error"] = np.maximum(df["Actual Rows"] / df["Plan Rows"] , df["Plan Rows"] / df["Actual Rows"])
    print(df[["Actual Rows", "Plan Rows", "q_error", "Query Name"]].head(20))


    plt.scatter(
        df["Actual Rows"],
        df["q_error"],
        alpha=0.6,
    )
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Table Size")
    plt.ylabel("Q Error")
    plt.title("Job: Actual Table Size Effect on Q Error")
    plt.grid(True, linestyle="--")
    # Add y = x reference line
    x_vals = df["Actual Rows"]
    plt.plot(
        x_vals,
        x_vals,
        linestyle="--",
        color="red",
        linewidth=1,
        label="q_error = table_size"
    )
    plt.legend()

    plt.show()


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
tpch15_df = pd.read_csv("query15tcph.csv")
tpch_df = pd.concat([tcphmain_df, tpch15_df], ignore_index=True)


job_df = pd.read_csv("jobqueries.csv")

job_no_loop = pd.read_csv("noNestedLoopsJob.csv")
#plot_join_error_boxplots(job_df)
#plot_type_of_join(job_df)
plot_join_error_boxplots(tpch_df)
#plot_cost_vs_time(job_df)
# plot_join_error_boxplots(job_no_loop)
#plot_join_error_boxplots(stack_df)
#q_error_table(job_df, stack_df, tpch_df)

#scatter_join_times("jobqueries.csv", "noNestedLoopsJob.csv")

#table_size_graph(job_df)
#table_size_graph(tpch_df)
#table_size_graph(stack_df)

