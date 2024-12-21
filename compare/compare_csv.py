import os
import pandas as pd
def compare_csv(you_csv, res_csv):
    time_intervels = res_csv['时间区间']
    you_time_intervels = you_csv['时间区间']
    diff_order_size = time_intervels.size - you_time_intervels.size
    # print(diff_order_size)

    if diff_order_size < 0:
        diff_set = you_time_intervels[you_time_intervels.isin(time_intervels)==False]
        # 超出订单记为错误订单
        you_csv = you_csv[~you_csv['时间区间'].isin(diff_set)]
        diff_order_size = abs(diff_set.size)

    for time in you_csv['时间区间']:
        # print(time)
        your_order = you_csv[you_csv['时间区间'] == time]
        res_order = res_csv[res_csv['时间区间'] == time]

        your_order_time = your_order['时间区间'].values[0]
        res_order_time = res_order['时间区间'].values[0]

        your_order = your_order.drop('时间区间', axis=1)
        res_order = res_order.drop('时间区间', axis=1)

        your_order_total_sum = float(your_order['主力净流入'].values[0])
        res_order_total_sum = float(res_order['主力净流入'].values[0])
        print(your_order_total_sum, res_order_total_sum)
        diff = abs(abs(your_order_total_sum) - abs(res_order_total_sum))
        # print(diff)
        if diff >= float(10) or your_order_time != res_order_time or pd.isna(diff) :
            diff_order_size = diff_order_size + 1

    score = (len(res_csv) - diff_order_size) / len(res_csv)

    return round(score * 100,2)



if __name__ == '__main__':
    # 你输出的文件
    your_output_folder_path = './mine'
    # 正确结果文件
    folder_path = "./ans"

    score = 0
    # 遍历文件夹内的所有文件
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            # 处理每个文件的逻辑
            your_res_csv = pd.read_csv(your_output_folder_path + "/" + file)
            res_csv = pd.read_csv(file_path)
            if file == '000001-时间间隔20min-results.csv':
                score += compare_csv(your_res_csv, res_csv) * 0.2
            if file == '000001-时间间隔10min-results.csv':
                score += compare_csv(your_res_csv, res_csv) * 0.2
            if file == '000166-时间间隔15min-results.csv':
                score += compare_csv(your_res_csv, res_csv) * 0.4
            if file == '300001-时间间隔30min-results.csv':
                score += compare_csv(your_res_csv, res_csv) * 0.4

    score = round(score,2)
    print(score)
    print(str(score) + "        " + str(score * 0.15))

