<template>
  <div>
    <!-- 页面头部 -->
    <page-header />

    <!-- 搜索表单 -->
    <div class="table-search-wrapper">
      <a-card :bordered="false">
        <a-form :model="queryState" @finish="onFinish">
          <a-row>
            <a-col flex="0 1 450px">
              <a-form-item name="app_name" label="应用名称" style="max-width: 300px;">
                <a-input v-model:value="queryState.app_name" placeholder="请输入应用名称" allowClear></a-input>
              </a-form-item>
            </a-col>
            <a-col flex="0 1 450px">
              <a-form-item name="script_name" label="脚本名称" style="max-width: 300px;">
                <a-input v-model:value="queryState.script_name" placeholder="请输入脚本名称" allowClear></a-input>
              </a-form-item>
            </a-col>
            <a-col flex="0 1 450px">
              <a-form-item name="is_parsed" label="是否已解析" style="max-width: 300px;">
                <a-select v-model:value="queryState.is_parsed" placeholder="全部" allowClear>
                  <a-select-option value="1">是</a-select-option>
                  <a-select-option value="0">否</a-select-option>
                </a-select>
              </a-form-item>
            </a-col>
          </a-row>
          <a-row>
            <a-col>
              <a-space>
                <a-button type="primary" html-type="submit" :loading="tableLoading">查询</a-button>
                <a-button @click="resetFields">重置</a-button>
                <a-button @click="handleImport">导入脚本</a-button>
                <a-button @click="handleParse">解析脚本</a-button>
              </a-space>
            </a-col>
          </a-row>
        </a-form>
      </a-card>
    </div>

    <!-- 表格区域 -->
    <div class="table-wrapper">
      <a-card title="SQL脚本列表" 
        :bordered="false" 
        :headStyle="{ borderBottom: 'none', padding: '20px 24px' }"
        :bodyStyle="{ padding: '0 24px', minHeight: 'calc(100vh - 400px)' }">
        <a-table
          :rowKey="record => record.id"
          :data-source="dataSource"
          :columns="columns"
          :loading="tableLoading"
          :pagination="pagination"
          :scroll="{ x: 500, y: 'calc(100vh - 450px)' }"
          :style="{ minHeight: '420px' }"
          @change="handleTableChange"
        >
          <template #bodyCell="{ column, record, index }">
            <template v-if="column.dataIndex === 'index'">
              <span>{{ (pagination.current - 1) * pagination.pageSize + index + 1 }}</span>
            </template>
            <template v-if="column.dataIndex === 'is_parsed'">
              <a-tag :color="record.is_parsed ? 'success' : 'default'">
                {{ record.is_parsed ? '是' : '否' }}
              </a-tag>
            </template>
            <template v-if="column.dataIndex === 'operation'">
              <a-space size="middle">
                <a @click="handleViewContent(record)">查看内容</a>
              </a-space>
            </template>
          </template>
        </a-table>
      </a-card>
    </div>

    <!-- 弹窗区域 -->
    <div class="modal-wrapper">
      <a-modal
        v-model:open="openModal"
        title="脚本内容"
        :width="800"
        :footer="null"
        style="top: 30px"
      >
        <a-typography>
          <pre>{{ selectedScript.script_content }}</pre>
        </a-typography>
      </a-modal>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { message } from 'ant-design-vue'
import { getScriptList, importScripts, parseScripts } from '@/api/tools/script'
import type { searchDataType, tableDataType } from './types';

// 表格数据
const dataSource = ref<tableDataType[]>([])
const tableLoading = ref<boolean>(false)

// 查询参数
const queryState = reactive<searchDataType>({
  app_name: null,
  script_name: null,
  is_parsed: null
})

// 分页配置
const pagination = reactive({
  current: 1,
  pageSize: 10,
  defaultPageSize: 10,
  showSizeChanger: true,
  total: dataSource.value.length,
  showTotal: (total, range) => `第 ${range[0]}-${range[1]} 条 / 总共 ${total} 条`
});

// 表格分页
const handleTableChange = (values: any) => {
  pagination.current = values.current;
  pagination.pageSize = values.pageSize;
  loadingData();
};


// 表格列定义
const columns = [
  {
    title: '序号',
    dataIndex: 'index',
    align: 'center',
    width: 60
  },
  // {
  //   title: 'ID',
  //   dataIndex: 'id',
  //   width: 60,
  //   align: 'center'
  // },
  {
    title: '应用名称',
    dataIndex: 'app_name',
    width: 100,
    align: 'left'
  },
  {
    title: '脚本名称',
    dataIndex: 'script_name',
    width: 500,
    align: 'left'
  },
  {
    title: '脚本路径',
    dataIndex: 'script_path',
    width: 800,
    align: 'left'
  },
  {
    title: '已解析',
    dataIndex: 'is_parsed',
    width: 60,
    align: 'center'
  },
  // {
  //   title: '更新时间',
  //   dataIndex: 'update_time',
  //   width: 180,
  //   align: 'center'
  // },
  // {
  //   title: '更新人',
  //   dataIndex: 'update_by',
  //   width: 120,
  //   align: 'center'
  // },
  {
    title: '操作',
    dataIndex: 'operation',
    width: 120,
    align: 'center'
  }
]

// 弹窗相关
const openModal = ref<boolean>(false)
const selectedScript = ref<tableDataType>({})

onMounted(() => {
  loadingData();
})

const onFinish = () => {
  pagination.current = 1;
  loadingData();
};

const loadingData = () => {
  tableLoading.value = true;
  let params = {};
  if(queryState.app_name){
    params['app_name'] = queryState.app_name
  }
  if(queryState.script_name){
    params['script_name'] = queryState.script_name
  }
  if(queryState.is_parsed){
    params['is_parsed'] = queryState.is_parsed == "1" ? true : false;
  }
  params['page_no'] = pagination.current
  params['page_size'] = pagination.pageSize

  getScriptList(params).then(response => {
    const result = response.data
    dataSource.value = result.data.items;
    pagination.total = result.data.total;
    pagination.current = result.data.page_no;
    pagination.pageSize = result.data.page_size;
  }).catch(error => {
    console.log(error);
  }).finally(() => {
    tableLoading.value = false;
  });
}


// 重置查询
const resetFields = () => {
  Object.keys(queryState).forEach((key: string) => {
    delete queryState[key];
  });
  pagination.current = 1;
  queryState.app_name = null;
  queryState.script_name = null;
  queryState.is_parsed = null;

  // selectedKeys.value = [];

  loadingData();
}


// 导入脚本
const handleImport = async () => {
  tableLoading.value = true;
  importScripts().then(response =>{
    const result = response.data
  }).catch(error => {
    console.log(error);
  }).finally(() => {
    tableLoading.value = false;
  });
}

// 导入脚本
const handleParse = async () => {
  tableLoading.value = true;
  parseScripts().then(response =>{
    const result = response.data
  }).catch(error => {
    console.log(error);
  }).finally(() => {
    tableLoading.value = false;
  });
}

// 查看脚本内容
const handleViewContent = (record: tableDataType) => {
  selectedScript.value = record
  openModal.value = true
}

</script>

<style lang="scss" scoped>
.table-search-wrapper {
  margin-block-end: 16px;
}

.scrollable-content {
  max-height: 200px;
  /* 设置最大高度 */
  overflow-y: auto;
  /* 添加垂直滚动条 */
  white-space: pre-wrap;
  /* 保留换行符并允许文本换行 */
}

.json-viewer {
  max-height: 300px;
  overflow-y: auto;
}
</style>