from typing import Tuple, List

pyCube_wc: List[Tuple[str, float]] = []
pyCube_cpu: List[Tuple[str, float]] = []
pyCube_mem: List[Tuple[str, float]] = []

baseline1_wc: List[Tuple[str, float]] = []
baseline1_cpu: List[Tuple[str, float]] = []
baseline1_mem: List[Tuple[str, float]] = []

baseline2_wc: List[Tuple[str, float]] = []
baseline2_cpu: List[Tuple[str, float]] = []
baseline2_mem: List[Tuple[str, float]] = []

baseline3_wc: List[Tuple[str, float]] = []
baseline3_cpu: List[Tuple[str, float]] = []
baseline3_mem: List[Tuple[str, float]] = []

with open("new_results", "r") as file:
    line: str = file.readline()
    while line:
        if "pyCube" in line:
            query_number: str = line[-3:-1]

            # Find WC time
            line = file.readline()
            index: int = line.find("seconds")
            time: str = line[index - 7:index]
            minute: int = int(time[0])
            seconds: int = int(time[2:4])
            centiseconds: int = int(time[5:])
            time_in_seconds: float = (minute * 60) + seconds + (centiseconds / 100)
            pyCube_wc.append((f"Q{query_number}", time_in_seconds))

            # Find CPU time
            # Find User CPU time
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("seconds")
            time = line[first_index + 1:second_index]
            u_cpu_time: float = float(time)

            # Find System CPU time
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("seconds")
            time = line[first_index + 1:second_index]
            sys_cpu_time: float = float(time)
            result = round(u_cpu_time + sys_cpu_time, 2)
            pyCube_cpu.append((f"Q{query_number}", result))

            # Find RSS
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("Kbytes")
            size = int(line[first_index + 1:second_index])
            pyCube_mem.append((f"Q{query_number}", size))

            file.readline()
            file.readline()
        elif "pandas" in line:
            query_number = line[12:14]
            baseline_number = int(line[-2])

            # Find WC time
            line = file.readline()
            index: int = line.find("seconds")
            time: str = line[index - 7:index]
            minute: int = int(time[0])
            seconds: int = int(time[2:4])
            centiseconds: int = int(time[5:])
            time_in_seconds: float = round((minute * 60) + seconds + (centiseconds / 100), 2)
            if baseline_number == 1:
                baseline1_wc.append((f"Q{query_number}", time_in_seconds))
            elif baseline_number == 2:
                baseline2_wc.append((f"Q{query_number}", time_in_seconds))
            elif baseline_number == 3:
                baseline3_wc.append((f"Q{query_number}", time_in_seconds))


            # Find CPU time
            # Find User CPU time
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("seconds")
            time = line[first_index + 1:second_index]
            u_cpu_time: float = float(time)

            # Find System CPU time
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("seconds")
            time = line[first_index + 1:second_index]
            sys_cpu_time: float = float(time)

            result = round(u_cpu_time + sys_cpu_time, 2)
            if baseline_number == 1:
                baseline1_cpu.append((f"Q{query_number}", result))
            elif baseline_number == 2:
                baseline2_cpu.append((f"Q{query_number}", result))
            elif baseline_number == 3:
                baseline3_cpu.append((f"Q{query_number}", result))

            # Find RSS
            line = file.readline()
            first_index = line.find(":")
            second_index = line.find("Kbytes")
            result = int(line[first_index + 1:second_index])
            if baseline_number == 1:
                baseline1_mem.append((f"Q{query_number}", result))
            elif baseline_number == 2:
                baseline2_mem.append((f"Q{query_number}", result))
            elif baseline_number == 3:
                baseline3_mem.append((f"Q{query_number}", result))

            file.readline()
            file.readline()

        line = file.readline()

    print(pyCube_wc)
    print(pyCube_cpu)
