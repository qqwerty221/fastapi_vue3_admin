import json
import os
import pandas as pd # 导入 pandas

from playwright.sync_api import sync_playwright, Page, BrowserContext

year = "2025"

for month in ['1','2','3','4']:
    # 获取当前文件的绝对路径
    folder_path = os.path.dirname(os.path.abspath(__file__))
    config_folder = os.path.join(folder_path, "config")
    config_file = os.path.join(config_folder, "cookies.json")
    # 将输出文件扩展名更改为 .xlsx
    export_file = os.path.join(folder_path, "export_" + month + ".xlsx")


    fee_page = "https://cloud.cmft.com/api/v1/cloud/bills/projects/dailybills?__limit=999999&__offset=0&year=" + year + "&month=" + month
    all_results = [] # 用于收集所有结果

    p_list = [
          ["AMH", "招商蛇口公寓BI"]
        , ["AMS", "投资性房地产资产管理系统"]
        , ["BDL-CMDP-BASIC", "cmsk-数据湖通用项目"]
        , ["CMSK-BAA", "2024年经营分析助手"]
        , ["CMSK-BAP", "大数据-招商蛇口经营分析项目"]
        , ["CHP", "大数据-招商蛇口游轮游轮数据分析项目"]
        , ["CSTM", "大数据-成本优化及分析大屏项目"]
        , ["CYWY", "大数据-持有物业数字化"]
        , ["REPORTING-TOOL", "投资数据分析"]
        , ["DMD", "大数据-招商蛇口数据资产项目"]
        , ["DOPA", "大数据-蛇口运营数据分析"]
        , ["FMCS", "大数据-财务对账项目"]
        , ["smartdata", "数据智能基础平台"]
        , ["IPBI", "招商产园BI项目"]
        , ["marketcloud", "思为营销云"]
        , ["PDA", "大数据-招商蛇口流程数据分析应用"]
        , ["PEFL", "大数据-招商蛇口红黑榜应用"]
        , ["PFO", "大数据-战发首发项目"]
        , ["CMSK-pmc", "项目管理协同平台"]
        , ["CMSK-POR", "大数据-招商蛇口项目作战室"]
        , ["SALE", "大数据-招商蛇口销售分析应用"]
        , ["YCYMB", "大数据-招商蛇口一城一模板数字化应用"]
        , ["cmcp", "建管委托方驾驶舱平台"]
        , ["DEPOSIT", "大数据-土地资金跟踪应用"]
        , ["HPM", "人效管理数字化项目"]
        , ["IMR", "大数据-城市投资回顾"]
        , ["QJDT", "招商蛇口全景地图"]
        , ["SXH", "思享汇"]
        , ["TeamBition", "TeamBition私有化"]
        , ["CRIC", "大数据-招商蛇口投资辅助"]
        , ["PBUSINESS", "大数据-非公开业务应用"]
    ]


    def login():
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # 可以设置 headless=True 以无头模式运行
            context = browser.new_context()
            with open(config_file, "r") as saved_cookies:
                sd_cookies = json.load(saved_cookies)

            context.add_cookies(sd_cookies)
            page = context.new_page()
            page.goto("https://cloud.cmft.com/#/login")
            print(page.title())  # 输出页面标题
            page.wait_for_url("https://cloud.cmft.com/#/portal")  # 替换为登录后的页面 URL

            # 获取并保存 Cookies
            cookies = context.cookies()
            with open(config_file, "w") as file:
                json.dump(cookies, file, indent=4)

            page2 = context.new_page()
            page2.goto("https://cloud.cmft.com/#/ecsAdminManage")
            page2.get_by_role("link", name="CMSK / AAS").click()

            for proj in p_list:
                app_code = proj[0]
                project_name = proj[0] + "-" + proj[1]
                loop(context, page2, app_code, project_name)

            # 在循环结束后将所有结果写入 Excel 文件
            if all_results:
                df = pd.DataFrame(all_results)
                df.to_excel(export_file, index=False, engine='openpyxl') # 使用 pandas 写入 Excel
                print(f"数据已成功导出到 {export_file}")
            else:
                print("没有收集到任何数据。")

            browser.close()


    def loop(o_context: BrowserContext, loop_page: Page, app_code: str, project_name: str):
        loop_page.get_by_text(project_name).click()
        loop_page.get_by_role("button", name="确定").click()

        page_l = o_context.new_page()
        page_l.goto(fee_page)

        output_file = os.path.join(config_folder, "export_" + app_code + ".json")
        content = page_l.text_content("body")
        # 直接写入获取到的文本内容，因为它已经是 JSON 字符串
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(content)

        page_l.close()
        extract_from_json(output_file)
        print(project_name)
        loop_page.get_by_role("link", name="CMSK / " + app_code).click()


    def extract_from_json(file_path):
        # 读取 JSON 文件
        with open(file_path, "r", encoding="utf-8") as file: # Changed mode to 'r'
            raw_content = file.read()
            # raw_content 已经是 JSON 字符串，直接解析
            data = json.loads(raw_content)

        # 提取目标字段并添加到全局列表
        for item in data.get("data", []):
            extracted = {
                "year_month": item.get("year_month"),
                "day": item.get("day"),
                "product_descriptor": item.get("product_descriptor", {}).get("name"),
                "name": item.get("project", {}).get("name"),
                "name_zh": item.get("project", {}).get("name_zh"),
                "origin_bill": item.get("origin_bill"),
                "bill": item.get("bill"),
            }
            all_results.append(extracted)

        # 移除 CSV 文件追加写入逻辑
        # with open(export_csv_file, "a", newline="", encoding="utf-8") as file:
        #     writer1 = csv.DictWriter(file, fieldnames=["year_month", "day", "product_descriptor", "name", "name_zh", "origin_bill", "bill"])
        #     writer1.writerows(results)

    login()
