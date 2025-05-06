import os
import subprocess


def count_files(directory):
    """统计目录中的文件数量（不包括.git目录）"""
    count = 0
    for root, dirs, files in os.walk(directory):
        if '.git' in dirs:
            dirs.remove('.git')
        count += len(files)
    return count


def clone_repository(app_name, git_url, app_dir):
    """克隆Git仓库到指定目录"""
    try:
        subprocess.run(["git", "clone", git_url, app_dir], 
                      check=True, capture_output=True, 
                      text=True, encoding='utf-8', errors='ignore')
    except Exception as e:
        print(f"[{app_name}] 克隆失败: {str(e)}")


def update_and_count_repos(base_dir):
    """更新仓库并统计文件数量"""
    if not os.path.isdir(base_dir):
        print(f"错误：仓库目录 '{base_dir}' 不存在")
        return

    repo_dirs = [d for d in os.listdir(base_dir) 
                if os.path.isdir(os.path.join(base_dir, d))]
    if not repo_dirs:
        print(f"仓库目录 '{base_dir}' 为空")
        return

    for item in repo_dirs:
        app_dir = os.path.join(base_dir, item)
        git_dir = os.path.join(app_dir, '.git')

        if os.path.isdir(git_dir):
            try:
                result = subprocess.run(["git", "pull"], 
                                      cwd=app_dir, check=True, 
                                      capture_output=True, 
                                      text=True, encoding='utf-8', 
                                      errors='ignore')
                if result.stderr.strip():
                    print(f"[{item}] 更新警告: {result.stderr.strip()}")
                file_count = count_files(app_dir)
                print(f"[{item}] 文件数: {file_count}")
            except Exception as e:
                print(f"[{item}] 更新失败: {str(e)}")


# 配置和仓库列表
folder_path = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.abspath(os.path.join(folder_path, "repositories"))
os.makedirs(base_dir, exist_ok=True)

repositories = [
    {"app": "cmsk_amh", "git": "https://code-inc.cmft.com/CMSK/AMH/cmsk-amh.git"},
    {"app": "cmsk_ams", "git": "https://code-inc.cmft.com/CMSK/AMS/cmsk-ams-data.git"},
    {"app": "cmsk_baa", "git": "https://code-inc.cmft.com/CMSK/CMSK-BAA/cmsk-baa.git"},
    {"app": "cmsk_bap", "git": "https://code-inc.cmft.com/CMSK/CMSK-BAP/cmsk-bap.git"},
    {"app": "cmsk_basic", "git": "https://code-inc.cmft.com/CMSK/BDL-CMDP-BASIC/cmsk-basic.git"},
    {"app": "cmsk_chp", "git": "https://code-inc.cmft.com/CMSK/CHP/cmsk-chp.git"},
    {"app": "cmsk_cstm", "git": "https://code-inc.cmft.com/CMSK/CSTM/cmsk-cstm.git"},
    {"app": "cmsk_cywy", "git": "https://code-inc.cmft.com/CMSK/CYWY/cmsk-cywy.git"},
    {"app": "cmsk_dfi", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-dfi-bigdata.git"},
    {"app": "cmsk_dmd", "git": "https://code-inc.cmft.com/CMSK/DMD/cmsk-dmd.git"},
    {"app": "cmsk_dopa", "git": "https://code-inc.cmft.com/CMSK/DOPA/cmsk-dopa.git"},
    {"app": "cmsk_fmcs", "git": "https://code-inc.cmft.com/CMSK/FMCS/cmsk-fmcs.git"},
    {"app": "cmsk_gentou", "git": "https://code-inc.cmft.com/CMSK/GTXT/cmsk-gentou-bigdata.git"},
    {"app": "cmsk_hobi", "git": "https://code-inc.cmft.com/CMSK/HOBI/cmsk-hobi.git"},
    {"app": "cmsk_hrbi", "git": "https://code-inc.cmft.com/CMSK/HRBI/cmsk-hrbi.git"},
    {"app": "cmsk_ias", "git": "https://code-inc.cmft.com/CMSK/ias/cmsk-ias-data.git"},
    {"app": "cmsk_idp", "git": "https://code-inc.cmft.com/CMSK/smartdata/cmsk-idp.git"},
    {"app": "cmsk_ipbi", "git": "https://code-inc.cmft.com/CMSK/IPBI/cmsk-ipbi.git"},
    {"app": "cmsk_marketcloud", "git": "https://code-inc.cmft.com/CMSK/marketcloud/cmsk-marketcloud.git"},
    {"app": "cmsk_mobi", "git": "https://code-inc.cmft.com/CMSK/MOBI/cmsk-mobi-bigdata.git"},
    {"app": "cmsk_pda", "git": "https://code-inc.cmft.com/CMSK/PDA/cmsk-pda-bigdata.git"},
    {"app": "cmsk_pdm", "git": "https://code-inc.cmft.com/CMSK/PDM/cmsk-pdm-bigdata.git"},
    {"app": "cmsk_pefl", "git": "https://code-inc.cmft.com/CMSK/PEFL/cmsk-pefl-bigdata.git"},
    {"app": "cmsk_pfo", "git": "https://code-inc.cmft.com/CMSK/PFO/cmsk-pfo-bigdata.git"},
    {"app": "cmsk_pmc", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-pmc-bigdata.git"},
    {"app": "cmsk_por", "git": "https://code-inc.cmft.com/CMSK/CMSK-POR/cmsk-por.git"},
    {"app": "cmsk_sale", "git": "https://code-inc.cmft.com/CMSK/SALE/cmsk-sale.git"},
    {"app": "cmsk_sdp", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-sdp.git"},
    {"app": "cmsk_ycymb", "git": "https://code-inc.cmft.com/CMSK/YCYMB/cmsk-ycymb-bigdata.git"},
    {"app": "cmsk_ycymb_bak", "git": "https://code-inc.cmft.com/CMSK/YCYMB/cmsk-ycymb-bak-bigdata.git"},
    {"app": "cmsk_cmcp", "git": "https://code-inc.cmft.com/CMSK/cmcp/cmsk-cmcp-bigdata.git"},
    {"app": "cmsk_esg", "git": "https://code-inc.cmft.com/CMSK/ESG/cmsk_esg_data.git"},
    {"app": "cmsk_super_app", "git": "https://code-inc.cmft.com/CMSK/SUPER-APP/cmsk-super-app-bigdata.git"}
]


# 主执行逻辑
for repo in repositories:
    app_name = repo["app"]
    git_url = repo["git"]
    app_dir = os.path.join(base_dir, app_name)

    if os.path.isdir(app_dir):
        git_dir_check = os.path.join(app_dir, '.git')
        if not os.path.isdir(git_dir_check):
            clone_repository(app_name, git_url, app_dir)
    else:
        clone_repository(app_name, git_url, app_dir)

update_and_count_repos(base_dir)
