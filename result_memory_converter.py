import re
import sys
from typing import List, Dict

query_names = [["pyCube_query11",
                "pyCube_query12",
                "pyCube_query13",
                "pyCube_query21",
                "pyCube_query22",
                "pyCube_query23",
                "pyCube_query31",
                "pyCube_query32",
                "pyCube_query33",
                "pyCube_query34",
                "pyCube_query41",
                "pyCube_query42",
                "pyCube_query43"],
               ["pandas_query11_baseline1",
                "pandas_query12_baseline1",
                "pandas_query13_baseline1",
                "pandas_query21_baseline1",
                "pandas_query22_baseline1",
                "pandas_query23_baseline1",
                "pandas_query31_baseline1",
                "pandas_query32_baseline1",
                "pandas_query33_baseline1",
                "pandas_query34_baseline1",
                "pandas_query41_baseline1",
                "pandas_query42_baseline1",
                "pandas_query43_baseline1"],
               ["pandas_query11_baseline2",
                "pandas_query12_baseline2",
                "pandas_query13_baseline2",
                "pandas_query21_baseline2",
                "pandas_query22_baseline2",
                "pandas_query23_baseline2",
                "pandas_query31_baseline2",
                "pandas_query32_baseline2",
                "pandas_query33_baseline2",
                "pandas_query34_baseline2",
                "pandas_query41_baseline2",
                "pandas_query42_baseline2",
                "pandas_query43_baseline2"],
               ["pandas_query11_baseline3",
                "pandas_query12_baseline3",
                "pandas_query13_baseline3",
                "pandas_query21_baseline3",
                "pandas_query22_baseline3",
                "pandas_query23_baseline3",
                "pandas_query31_baseline3",
                "pandas_query32_baseline3",
                "pandas_query33_baseline3",
                "pandas_query34_baseline3",
                "pandas_query41_baseline3",
                "pandas_query42_baseline3",
                "pandas_query43_baseline3"]]


def parse_results(file_name) -> Dict[str, List[float]]:
    result_dict = {}
    with (open(file_name, "r") as file):
        for line in file:
            query_name = line[:-1]
            times = file.readline()
            if times[:-1] == "Command terminated by signal 9":
                file.readline()
                continue
            regex = re.compile('[0-9]+')
            mem_size_str = regex.findall(times)[0]
            mem_size = float(mem_size_str) / 1000000
            if query_name in result_dict.keys():
                result_dict[query_name].append(mem_size)
            else:
                result_dict[query_name] = [mem_size]
    return result_dict


def remove_largest_and_smallest_element(res_dict):
    for k, v in res_dict.items():
        res_dict[k].remove(max(v))
        res_dict[k].remove(min(v))


def average_values(res_dict):
    for k, v in res_dict.items():
        values = res_dict[k]
        res_dict[k] = round(sum(values) / len(values), 3)


c = ["bblue", "rred", "ggreen", "ppurple"]
query_flight_names = ["Q11", "Q12", "Q13", "Q21", "Q22", "Q23", "Q31", "Q32", "Q33", "Q34", "Q41", "Q42", "Q43"]


def print_plot(res_dict):
    print(r"\begin{tikzpicture}")
    print(r"    \begin{axis}[")
    print(r"        width  = 0.8*\textwidth,")
    print(r"        height = 4cm,")
    print(r"        major x tick style = transparent,")
    print(r"        ybar=1.2pt,")
    print(r"        bar width=4pt,")
    print(r"        ymajorgrids = true,")
    print(r"        ylabel = {Memory (GB)},")
    print(r"        y label style = {font=\footnotesize,at={(-0.005,0.5)}},")
    print(r"        symbolic x coords={Q11,Q12,Q13,Q21,Q22,Q23,Q31,Q32,Q33,Q34,Q41,Q42,Q43},")
    print(r"        xtick = data,")
    print(r"        scaled y ticks = false,")
    print(r"        ymin=0")
    print(r"    ]")

    for i, names in enumerate(query_names):
        print(f"        \\addplot[style={{{c[i]},fill={c[i]},mark=none}}]")
        strs = ["coordinates ", r"{"]
        for j, name in enumerate(names):
            if name in res_dict.keys():
                strs.append(f"({query_flight_names[j]}, {res_dict[name]}) ")
        strs.append(r"};")
        print("".join(strs))

    for i, name in enumerate(query_names[0]):
        print(f"        \\node[above left,rotate=15,yshift=.51cm,xshift=2pt,style={{{c[0]}}}] (Q{name[-2:]}) at (axis cs:Q{name[-2:]}, {res_dict[name]}) {{{res_dict[name]}}};")
        print(f"        \\draw[->,style={{{c[0]}}},bend left] (Q{name[-2:]}.south) to[xshift=-8pt,yshift=2pt] (axis cs:Q{name[-2:]}, {res_dict[name]}.north);")

    print(r"    \end{axis}")
    print(r"\end{tikzpicture}")


if sys.argv and len(sys.argv) == 2:
    mem_results = parse_results(sys.argv[1])
    remove_largest_and_smallest_element(mem_results)
    average_values(mem_results)
    print_plot(mem_results)
