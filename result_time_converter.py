import re
import sys


def parse_results(file_name):
    python_dict = {}
    db_dict = {}
    with (open(file_name, "r") as file):
        for line in file:
            query_name = line[:-3]
            times = file.readline()
            regex = re.compile("[0-9]+")
            python_time_str, db_time_str = regex.findall(times)
            python_time = float(python_time_str) / 1000000000
            db_time = float(db_time_str) / 1000000000
            if query_name in python_dict.keys() and query_name in db_dict.keys():
                python_dict[query_name].append(python_time)
                db_dict[query_name].append(db_time)
            elif query_name not in python_dict.keys() and query_name not in db_dict.keys():
                python_dict[query_name] = [python_time]
                db_dict[query_name] = [db_time]
            else:
                print("This should not happen")
            file.readline()
    return python_dict, db_dict


def remove_largest_and_smallest_element(python_dict, db_dict) -> None:
    for k, v in python_dict.items():
        python_dict[k].remove(max(v))
        python_dict[k].remove(min(v))
    for k, v in db_dict.items():
        db_dict[k].remove(max(v))
        db_dict[k].remove(min(v))


# combine the two methods
def average_values(python_dict, db_dict) -> None:
    for k, v in python_dict.items():
        values = python_dict[k]
        python_dict[k] = [round(sum(values) / len(values), 3)]
    for k, v in db_dict.items():
        values = db_dict[k]
        db_dict[k] = [round(sum(values) / len(values), 3)]


def get_query_names(flight_no: int) -> list[list[str]]:
    if flight_no == 3:
        return [
                [f"pyCube_query{flight_no}1", f"pyCube_query{flight_no}2", f"pyCube_query{flight_no}3", f"pyCube_query{flight_no}4"],
                [f"pandas_query{flight_no}1_baseline1", f"pandas_query{flight_no}2_baseline1", f"pandas_query{flight_no}3_baseline1", f"pandas_query{flight_no}4_baseline1"],
                [f"pandas_query{flight_no}1_baseline2", f"pandas_query{flight_no}2_baseline2", f"pandas_query{flight_no}3_baseline2", f"pandas_query{flight_no}4_baseline2"],
                [f"pandas_query{flight_no}1_baseline3", f"pandas_query{flight_no}2_baseline3", f"pandas_query{flight_no}3_baseline3", f"pandas_query{flight_no}4_baseline3"],
        ]
    else:
        return [
            [f"pyCube_query{flight_no}1", f"pyCube_query{flight_no}2", f"pyCube_query{flight_no}3"],
            [f"pandas_query{flight_no}1_baseline1", f"pandas_query{flight_no}2_baseline1", f"pandas_query{flight_no}3_baseline1"],
            [f"pandas_query{flight_no}1_baseline2", f"pandas_query{flight_no}2_baseline2", f"pandas_query{flight_no}3_baseline2"],
            [f"pandas_query{flight_no}1_baseline3", f"pandas_query{flight_no}2_baseline3", f"pandas_query{flight_no}3_baseline3"],
        ]


def print_start(flight_no):
    if flight_no == 3:
        print(r"\begin{tikzpicture}[")
        print(r"  every axis/.style={")
        print(r"    ymajorgrids = true,")
        print(r"    major x tick style = transparent,")
        print(r"    xtick = data,")
        print(f"    xlabel = {{QF {flight_no}}},")
        print(r"    enlarge x limits=0.25,")
        print(r"    symbolic x coords={")
        print(f"      Q{flight_no}1,")
        print(f"      Q{flight_no}2,")
        print(f"      Q{flight_no}3,")
        print(f"      Q{flight_no}4,")
        print(r"    },")
        print(r"    width  = 0.4*\textwidth,")
        print(r"    height = 4cm,")
        print(r"    ylabel = {Runtime (s)},")
        print(r"    y label style = {font=\footnotesize,at={(-0.05,0.5)}},")
        print(r"    ybar stacked,")
        print(r"    ybar=1.2pt,")
        print(r"    ymin=0,")
        print(r"    ymax=35,")
        print(r"    scaled y ticks = false,")
        print(r"    bar width=4pt,")
        print(r"    legend cell align=left,")
        print(r"    legend style={")
        print(r"            at={(1,1.05)},")
        print(r"            anchor=south east,")
        print(r"            column sep=1ex")
        print(r"    },")
        print(r"  },")
        print(r"]")
    else:
        print(r"\begin{tikzpicture}[")
        print(r"  every axis/.style={")
        print(r"    ymajorgrids = true,")
        print(r"    major x tick style = transparent,")
        print(r"    xtick = data,")
        print(f"    xlabel = {{QF {flight_no}}},")
        print(r"    enlarge x limits=0.25,")
        print(r"    symbolic x coords={")
        print(f"      Q{flight_no}1,")
        print(f"      Q{flight_no}2,")
        print(f"      Q{flight_no}3,")
        print(r"    },")
        print(r"    width  = 0.4*\textwidth,")
        print(r"    height = 4cm,")
        print(r"    ylabel = {Runtime (s)},")
        print(r"    y label style = {font=\footnotesize,at={(-0.05,0.5)}},")
        print(r"    ybar stacked,")
        print(r"    ybar=1.2pt,")
        print(r"    ymin=0,")
        print(r"    ymax=35,")
        print(r"    scaled y ticks = false,")
        print(r"    bar width=4pt,")
        print(r"    legend cell align=left,")
        print(r"    legend style={")
        print(r"            at={(1,1.05)},")
        print(r"            anchor=south east,")
        print(r"            column sep=1ex")
        print(r"    },")
        print(r"  },")
        print(r"]")


def print_end():
    print(r"\end{tikzpicture}")
    print()


def print_query_flight(result_dict):
    c = [
        ["bblue", "bbblue"],
        ["rred", "rrred"],
        ["ggreen", "gggreen"],
        ["ppurple", "pppurple"],
    ]
    for i in range(1, 5):
        print_start(i)
        if i == 3:
            for j, a in enumerate(get_query_names(i)):
                if j == 0:
                    print(r"\begin{axis}[bar shift=-9pt]")
                    print(f"    \\addplot[style={{color={c[j][0]},fill={c[j][0]}}}]")
                    print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][0]}) (Q{i}2, {result_dict[a[1]][0]}) (Q{i}3, {result_dict[a[2]][0]}) (Q{i}4, {result_dict[a[3]][0]})}};")
                    print(r"    \addplot[style={color=bbblue,fill=bbblue}]")
                    print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][1]}) (Q{i}2, {result_dict[a[1]][1]}) (Q{i}3, {result_dict[a[2]][1]}) (Q{i}4, {result_dict[a[3]][1]})}};")
                    print(r"\end{axis}")
                else:
                    if a[0] in result_dict.keys():
                        print(f"\\begin{{axis}}[bar shift={6 * j - 9}pt,hide axis]")
                        print(f"    \\addplot+[style={{color={c[j][0]},fill={c[j][0]}}}]")
                        print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][0]}) (Q{i}2, {result_dict[a[1]][0]}) (Q{i}3, {result_dict[a[2]][0]}) (Q{i}4, {result_dict[a[3]][0]})}};")
                        print(f"    \\addplot+[style={{color={c[j][1]},fill={c[j][1]}}}]")
                        print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][1]}) (Q{i}2, {result_dict[a[1]][1]}) (Q{i}3, {result_dict[a[2]][1]}) (Q{i}4, {result_dict[a[3]][1]})}};")
                        print(r"\end{axis}")
                    else:
                        print()
        else:
            for j, a in enumerate(get_query_names(i)):
                if j == 0:
                    print(r"\begin{axis}[bar shift=-9pt]")
                    print(f"    \\addplot[style={{color={c[j][0]},fill={c[j][0]}}}]")
                    print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][0]}) (Q{i}2, {result_dict[a[1]][0]}) (Q{i}3, {result_dict[a[2]][0]})}};")
                    print(r"    \addplot[style={color=bbblue,fill=bbblue}]")
                    print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][1]}) (Q{i}2, {result_dict[a[1]][1]}) (Q{i}3, {result_dict[a[2]][1]})}};")
                    print(r"\end{axis}")
                else:
                    if a[0] in result_dict.keys():
                        print(f"\\begin{{axis}}[bar shift={6*j - 9}pt,hide axis]")
                        print(f"    \\addplot+[style={{color={c[j][0]},fill={c[j][0]}}}]")
                        print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][0]}) (Q{i}2, {result_dict[a[1]][0]}) (Q{i}3, {result_dict[a[2]][0]})}};")
                        print(f"    \\addplot+[style={{color={c[j][1]},fill={c[j][1]}}}]")
                        print(f"        coordinates {{(Q{i}1, {result_dict[a[0]][1]}) (Q{i}2, {result_dict[a[1]][1]}) (Q{i}3, {result_dict[a[2]][1]})}};")
                        print(r"\end{axis}")
                    else:
                        print()
        print_end()


if sys.argv and len(sys.argv) == 2:
    python_results, db_results = parse_results(sys.argv[1])
    remove_largest_and_smallest_element(python_results, db_results)
    average_values(python_results, db_results)

    result = {}
    for key, value in zip(python_results.items(), db_results.items()):
        result[key[0]] = [*key[1], *value[1]]

    print_query_flight(result)
    sys.exit(0)

