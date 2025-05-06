import request from '@/utils/request.js'

// 获取脚本列表
export function getScriptList(body) {
  return request({
    url: '/api/v1/tools/script/list',
    method: 'get',
    data: body
  })
}

// 导入脚本
export function importScripts() {
  return request({
    url: '/api/v1/tools/script/import',
    method: 'post'
  })
}