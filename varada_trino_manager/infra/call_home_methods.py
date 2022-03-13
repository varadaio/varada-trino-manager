
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from re import findall as re_findall
import re
import os
import json
import collections
import datetime
import time
from .s3 import S3URL
from click import echo


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                         Parse metrics

example of metrics line in slog:
2022-02-08T07:16:35.437Z	INFO	Timer-1	METRICS-DUMP	{"stats":{"dictionary.varada":{"dictionaries_size":"-55 (0)",
                                                            "dictionary_read_elements_count":"+10 (21)","dictionary_entries":"-1 (0)",
                                                            "dictionaries_varlen_str_size":"-55 (0)",
                                                            "dictionary_loaded_elements_count":"+1 (14)"},
                                                            "dispatcherPageSource.varada":{"varada_collect_columns":"+10 (2197)",
                                                            "cached_total_rows":"+10 (127390974)","cached_files":"+10 (5957)",
                                                            "prefilled_collect_columns":"+10 (3496)",
                                                            "prefilled_collect_bytes":"+5 (34843226)",
                                                            "varada_match_columns":"+10 (3853)","cached_varada_success_files":"+10 (5779)",
                                                            "cached_read_rows":"+1 (37351331)"},"warmup-exporter.varada":{"export_disabled":"+10 (27289)"},
                                                            "worker-task-executor.varada":{"task_scheduled":"+10 (36068)","task_finished":"+10 (36068)"}},
                                                            "catalog":"varada","worker-nodes":1,"timestamp":1644304595437}

'''


def get_stats(line_splits):
    if len(line_splits[0]) > 1 and "stats\":" in line_splits[0][1]:
        return line_splits[0][1]
    return None


def get_timestamp(line_splits):
    if len(line_splits[0]):
        return line_splits[0][0]
    return None


def get_val(vals, pos):
    if len(vals) > pos:
        return vals[pos]
    return None


def get_slog_metrics(slog_files: list, frequency_minutes: int, start_time, end_time, delta_metrics):
    catalog_ts_jsons = collections.defaultdict(lambda: collections.defaultdict(dict))
    for slog in slog_files:
        for line in slog.split('\n'):
            if "METRICS-DUMP" in line:
                line_splits = re_findall(r'([\S]+)[\s]+INFO.*METRICS-DUMP[\s]+(.*)', line)  # re_findall returns array of strings, we need the line with stat in the second string
                json_stats = get_stats(line_splits)
                if json_stats:
                    json_data = json.loads(json_stats)
                    timestamp = json_data.get('timestamp', get_timestamp(line_splits))
                    if not timestamp:
                        continue
                    timestamp /= 1000  # seconds
                    timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime('%m/%d/%Y %H:%M')

                    if timestamp < start_time or timestamp > end_time:
                        continue
                    timestamp = int(timestamp / (60 * frequency_minutes))  # sum_minutes
                    catalog = json_data.get('catalog', 'varada')

                    stats: dict = json_data['stats']
                    for group_tuple in stats.items():
                        group_name_with_catalog = get_val(group_tuple, 0)
                        group_name_splits = get_val(re_findall(r"([^\.]+)", group_name_with_catalog), 0)
                        group_name = group_name_splits
                        tup_dict = get_val(group_tuple, 1)
                        if not tup_dict:
                            continue
                        metrics = dict(tup_dict).items()
                        for metric_tuple in metrics:
                            metric_name = get_val(metric_tuple, 0)
                            metric_value = get_val(metric_tuple, 1)
                            values = re_findall(r"([\+\-][\d]+).*\(([\d]+)\)", metric_value)
                            if len(values) == 0:
                                continue
                            if metric_name in delta_metrics:
                                val = int(get_val(values[0], 1))
                            else:
                                val = int(get_val(values[0], 0))
                            keyname = f"{group_name}-{metric_name}"
                            if catalog_ts_jsons.get(catalog) is None:
                                catalog_ts_jsons[catalog] = collections.defaultdict(dict)
                            if catalog_ts_jsons.get(catalog).get(timestamp) is None:
                                catalog_ts_jsons[catalog][timestamp] = {"timestamp": timestamp_str}
                            if catalog_ts_jsons[catalog][timestamp].get(keyname) is None:
                                catalog_ts_jsons[catalog][timestamp][keyname] = val
                            else:
                                catalog_ts_jsons[catalog][timestamp][keyname] += val

    return catalog_ts_jsons


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Draw graphs
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


def get_x_y(key: str, ts_keys: collections.defaultdict(dict), max_samples: int):
    vals = []
    cols = []
    for samples_count, (ts, dictkeys) in enumerate(ts_keys.items()):
        if samples_count == max_samples:
            break
        cols.append(dictkeys["timestamp"])
        fillkey = False
        for key_dict, val in dictkeys.items():
            if key_dict == key:
                vals.append(val)
                fillkey = True
        if not fillkey:
            vals.append(0)

    return cols, vals


def draw_graph(catalog: str, ts_dict: str, keys: list, name: str, title: str, out_dir: str, max_samples: int):
    fig, ax = plt.subplots()
    plt.xticks(rotation=90)

    ax.set_title(f"{name}-{catalog}-{title}")
    for key in keys:
        x, y = get_x_y(key, ts_dict, max_samples)
        ax.plot(x, y)
        ax.legend(keys)

    # after plotting the data, format the labels
    current_values = plt.gca().get_yticks()
    ticks_loc = ax.get_yticks().tolist()
    ax.yaxis.set_major_locator(mticker.FixedLocator(ticks_loc))
    plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

    plt.savefig(f"{out_dir}/{name}-{catalog}-{title}")
    plt.close()


def grep_slog_files(node, slog_files, grep_str: str, file, start_time, end_time):
    print_node(node, file)
    for slog in slog_files:
        for line in slog.split('\n'):
            if re.search(grep_str, line):
                logtime = time.mktime(datetime.datetime.strptime(line.split('Z')[0], "%Y-%m-%dT%H:%M:%S.%f").timetuple())  # 2022-02-08T06:39:23.787Z
                if logtime < start_time or logtime > end_time:
                    continue
                file.write(f"{line}\n")


def print_node(folder, file):
    file.write("---------------------------------------------\n")
    file.write(f"Node  {folder}\n")
    file.write("---------------------------------------------\n")


def show_metrics(call_config, files, start_time, end_time, out_dir, node_title):
    json_dicts = get_slog_metrics(files, call_config["granularity_minutes"], start_time, end_time, call_config["delta_metrics"])
    for name, vals in call_config["graphs_keys"].items():
        if name in call_config["graphs"]:
            for catalog, dict_ts in json_dicts.items():
                draw_graph(catalog, dict_ts, vals, name, node_title, out_dir, call_config["max_samples"])


def run(config_json: str):

    with open(config_json) as f:
        call_config = json.load(f)

    s3location = S3URL(call_config["s3_call_home"])
    echo(f"Call home for node {s3location}")

    start_time = time.mktime(datetime.datetime.strptime(call_config["start_time"], "%m/%d/%Y %H:%M").timetuple())
    end_time = time.mktime(datetime.datetime.strptime(call_config["end_time"], "%m/%d/%Y %H:%M").timetuple())

    out_dir = call_config["output_dir"]
    error = call_config["error"]
    audit = call_config["audit"]

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    if error:
        file_error = open(f"{out_dir}/error.log", 'w')
    if audit:
        file_audit = open(f"{out_dir}/audit.log", 'w')

    cluster_files = []
    for folder in s3location.glob_folders():
        node = f"{str(folder).split('/')[-2]}"
        slog_files = []
        for slog in (folder / 'server*').glob():
            text_file = slog.download_text()
            slog_files.append(text_file)
            cluster_files.append(text_file)

        if audit:
            grep_slog_files(node, slog_files, "AUDIT", file_audit, start_time, end_time)
        if error:
            grep_slog_files(node, slog_files, "ERROR", file_error, start_time, end_time)

        if call_config["each_node"]:
            show_metrics(call_config, slog_files, start_time, end_time, out_dir, node)

    if call_config["all_clusters"]:
        show_metrics(call_config, cluster_files, start_time, end_time, out_dir, "all-cluster")

