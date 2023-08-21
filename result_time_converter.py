import re
import sys


def parse_results():
    python_dict = {}
    db_dict = {}
    with (open("tmp.test", "r") as file):
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
        python_dict[k] = [sum(values) / len(values)]
    for k, v in db_dict.items():
        values = db_dict[k]
        db_dict[k] = [sum(values) / len(values)]


def print_query_flight(result_dict, flight_no):
    pyCube_query11 = "pandas_query11_baseline1"
    pyCube_query12 = "pandas_query12_baseline1"
    pyCube_query13 = "pandas_query13_baseline1"

    print(r"\addplot+[ybar]")
    print(f"    plot coordinates {{(Q{flight_no}1, {result_dict[pyCube_query11][0]}) (Q{flight_no}2, {result_dict[pyCube_query12][0]}) (Q{flight_no}3, {result_dict[pyCube_query13][0]})}}")

    print(r"\addplot+[ybar]")
    print(f"    plot coordinates {{(Q{flight_no}1, {result_dict[pyCube_query11][1]}) (Q{flight_no}2, {result_dict[pyCube_query12][1]}) (Q{flight_no}3, {result_dict[pyCube_query13][1]})}}")

if sys.argv and len(sys.argv) == 1:
    python_results, db_results = parse_results()
    # remove_largest_and_smallest_element(python_results, db_results)
    average_values(python_results, db_results)

    result = {}
    for key, value in zip(python_results.items(), db_results.items()):
        result[key[0]] = [*key[1], *value[1]]

    ## Remember to round the value of the results
    print(result)
    print_query_flight(result, 1)
    # print(python_results)
    # print(db_results)

    # print(list(zip(python_results.values(), db_results.values())))
    # print(len(list(zip(python_results.values(), db_results.values()))))

