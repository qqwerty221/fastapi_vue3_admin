import os
import shutil
import subprocess


def count_files(directory):
    """统计目录中的文件数量（不包括 .git 目录）"""
    count = 0
    for root, dirs, files in os.walk(directory):
        if '.git' in dirs:
            dirs.remove('.git')
        count += len(files)
    return count


def clone_repository(app_name, git_url, app_dir):
    """克隆 Git 仓库到指定目录"""
    try:
        result = subprocess.run(["git", "clone", "--quiet", git_url, app_dir],
                                check=True, capture_output=True,
                                text=True, encoding='utf-8', errors='ignore')
        if result.stdout.strip():
            print(f"[{app_name}] 克隆成功: {result.stdout.strip()}")
        if result.stderr.strip():
            print(f"[{app_name}] 克隆警告: {result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"[{app_name}] 克隆失败: {e.stderr.strip()}")
    except Exception as e:
        print(f"[{app_name}] 发生未知错误: {str(e)}")


def update_and_count_repos(base_dir):
    """更新仓库并统计文件数量"""
    if not os.path.isdir(base_dir):
        print(f"错误：仓库目录 '{base_dir}' 不存在")
        return

    repo_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

    if not repo_dirs:
        print(f"仓库目录 '{base_dir}' 为空")
        return

    for item in repo_dirs:
        app_dir = os.path.join(base_dir, item)
        git_dir = os.path.join(app_dir, '.git')

        if os.path.isdir(git_dir):
            try:
                result = subprocess.run(["git", "pull", "--quiet"], cwd=app_dir, check=True,
                                        capture_output=True, text=True, encoding='utf-8', errors='ignore')

                if result.stdout.strip():
                    print(f"[{item}] 更新信息: {result.stdout.strip()}")

                if result.stderr.strip():
                    print(f"[{item}] 更新警告: {result.stderr.strip()}")

                file_count = count_files(app_dir)
                print(f"[{item}] 文件数: {file_count}")
            except subprocess.CalledProcessError as e:
                print(f"[{item}] 更新失败: {e.stderr.strip()}")
            except Exception as e:
                print(f"[{item}] 发生未知错误: {str(e)}")
    print("update_complete!")


def init_and_clone(repositories, base_dir):
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
    print("init complete")

def reset_git(repositories, base_dir):
    commands = [
        "git reset --hard --quiet ",  # 删除所有文件
        "git push origin main --force --quiet "  # 推送到远程仓库
    ]

    for repo in repositories:
        app_name = repo["app"]
        app_dir = os.path.join(base_dir, app_name)

        try:
            for index, cmd in enumerate(commands, start=1):
                if index == 1:
                    cmd = cmd + repo["init"]
                subprocess.run(cmd, cwd=app_dir, shell=True, check=True)
            print(app_name + '已清理')
        except Exception as e:
            print(app_name + '异常无法清理')
            continue


# 配置和仓库列表
folder_path = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.abspath(os.path.join(folder_path, "repositories"))
os.makedirs(base_dir, exist_ok=True)

repositories = [
    {"app": "cmsk_cywy", "git": "https://code-inc.cmft.com/CMSK/CYWY/cmsk-cywy.git", "init": "41d98551"},
    {"app": "cmsk_amh", "git": "https://code-inc.cmft.com/CMSK/AMH/cmsk-amh.git", "init": "a108fb7c"},
    {"app": "cmsk_ams", "git": "https://code-inc.cmft.com/CMSK/AMS/cmsk-ams-data.git", "init": "8b2c8af3"},
    {"app": "cmsk_baa", "git": "https://code-inc.cmft.com/CMSK/CMSK-BAA/cmsk-baa.git", "init": "04c9adc2"},
    {"app": "cmsk_bap", "git": "https://code-inc.cmft.com/CMSK/CMSK-BAP/cmsk-bap.git", "init": "9bda7b68"},
    {"app": "cmsk_basic", "git": "https://code-inc.cmft.com/CMSK/BDL-CMDP-BASIC/cmsk-basic.git", "init": "c50593fa"},
    {"app": "cmsk_chp", "git": "https://code-inc.cmft.com/CMSK/CHP/cmsk-chp.git", "init": "a924ab9a"},
    {"app": "cmsk_dfi", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-dfi-bigdata.git", "init": "49b26f19"},
    {"app": "cmsk_dmd", "git": "https://code-inc.cmft.com/CMSK/DMD/cmsk-dmd.git", "init": "d8365674"},
    {"app": "cmsk_dopa", "git": "https://code-inc.cmft.com/CMSK/DOPA/cmsk-dopa.git", "init": "e9a844a4"},
    {"app": "cmsk_fmcs", "git": "https://code-inc.cmft.com/CMSK/FMCS/cmsk-fmcs.git", "init": "516225a0"},
    {"app": "cmsk_ias", "git": "https://code-inc.cmft.com/CMSK/ias/cmsk-ias-data.git", "init": "3dc5b8dd"},
    {"app": "cmsk_pmc", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-pmc-bigdata.git", "init": "4f79c8d1"},
    {"app": "cmsk_por", "git": "https://code-inc.cmft.com/CMSK/CMSK-POR/cmsk-por.git", "init": "b68ad0a4"},
    {"app": "cmsk_sale", "git": "https://code-inc.cmft.com/CMSK/SALE/cmsk-sale.git", "init": "b7fa33b2"},
    {"app": "cmsk_sdp", "git": "https://code-inc.cmft.com/CMSK/SDP/cmsk-sdp.git", "init": "1bffc653"},
    {"app": "cmsk_cstm", "git": "https://code-inc.cmft.com/CMSK/CSTM/cmsk-cstm.git", "init": "c5704aa3"},
    {"app": "cmsk_gentou", "git": "https://code-inc.cmft.com/CMSK/GTXT/cmsk-gentou-bigdata.git", "init": "0e7bc474"},
    {"app": "cmsk_hobi", "git": "https://code-inc.cmft.com/CMSK/HOBI/cmsk-hobi.git", "init": "57935a0f"},
    {"app": "cmsk_hrbi", "git": "https://code-inc.cmft.com/CMSK/HRBI/cmsk-hrbi.git", "init": "9a5b0d0b"},
    {"app": "cmsk_idp", "git": "https://code-inc.cmft.com/CMSK/smartdata/cmsk-idp.git", "init": "063be414"},
    {"app": "cmsk_ipbi", "git": "https://code-inc.cmft.com/CMSK/IPBI/cmsk-ipbi.git", "init": "d5e87b7d"},
    {"app": "cmsk_mobi", "git": "https://code-inc.cmft.com/CMSK/MOBI/cmsk-mobi-bigdata.git", "init": "b8db9744"},
    {"app": "cmsk_pda", "git": "https://code-inc.cmft.com/CMSK/PDA/cmsk-pda-bigdata.git", "init": "5a5e3b0b"},
    {"app": "cmsk_pdm", "git": "https://code-inc.cmft.com/CMSK/PDM/cmsk-pdm-bigdata.git", "init": "c2145f73"},
    {"app": "cmsk_pefl", "git": "https://code-inc.cmft.com/CMSK/PEFL/cmsk-pefl-bigdata.git", "init": "e4aba9e2"},
    {"app": "cmsk_marketcloud", "git": "https://code-inc.cmft.com/CMSK/marketcloud/cmsk-marketcloud.git", "init": "f2c18ddd"},
    {"app": "cmsk_pfo", "git": "https://code-inc.cmft.com/CMSK/PFO/cmsk-pfo-bigdata.git", "init": "9f71bb7c"},
    {"app": "cmsk_ycymb", "git": "https://code-inc.cmft.com/CMSK/YCYMB/cmsk-ycymb-bigdata.git", "init": "24ef2eeb"},
    {"app": "cmsk_ycymb_bak", "git": "https://code-inc.cmft.com/CMSK/YCYMB/cmsk-ycymb-bak-bigdata.git", "init": "1754f8cb"},
    {"app": "cmsk_esg", "git": "https://code-inc.cmft.com/CMSK/ESG/cmsk_esg_data.git", "init": "07df9a4a"},
    {"app": "cmsk_cmcp", "git": "https://code-inc.cmft.com/CMSK/cmcp/cmsk-cmcp-bigdata.git", "init": "5bbcce5b"},
    {"app": "cmsk_super_app", "git": "https://code-inc.cmft.com/CMSK/SUPER-APP/cmsk-super-app-bigdata.git", "init": "d8f4dec0"},
]

# 初始化
init_and_clone(repositories,base_dir)
# 更新
update_and_count_repos(base_dir)
# 清空
# reset_git(repositories, base_dir)