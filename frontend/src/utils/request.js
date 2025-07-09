import axios from "axios";
import storage from "store";
import router from "@/router";
import { getNewToken } from "@/api/system/auth";
import { save_token } from "@/utils/util";
import notification from "ant-design-vue/es/notification";
import { message } from 'ant-design-vue';

// 创建 axios 实例
const request = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL,
  timeout: Number(import.meta.env.VITE_TIMEOUT), // 请求超时时间
});

// 异常拦截处理器
const errorHandler = async (error) => {
  // console.error(error);

  if (!error.response) {
    return Promise.reject(error);
  }

  const data = error.response.data;
  // 从 localstorage 获取 access_token
  const access_token = storage.get("Access-Token");
  // || data.msg === "用户未登录或登录已过期"
  if (data.status_code === 403 || data.msg.includes("请重新登录")) {
    storage.remove("Access-Token");
    storage.remove("Refresh-Token");

    message.error(data.msg);
    router.push("/login");
    return Promise.reject(error);
  }

  if (data.status_code === 401) {
    if (!access_token) {
      message.error(data.msg);
      router.push("/login");
      return Promise.reject(error);
    }

    const refresh_token = storage.get("Refresh-Token");
    storage.remove("Access-Token");
    storage.remove("Refresh-Token");

    return getNewToken({ refresh_token: refresh_token })
      .then((response) => {
        const result = response.data;
        if (result.status_code === 200) {
          save_token(
            result.data.access_token,
            result.data.refresh_token,
            result.data.expires_in
          );
          return request(error.response.config).then((response) => response);
        }
        message.error(result.msg);
        router.push("/login");
        return Promise.reject(error);
      })
      .catch((error) => {
        message.error(data.msg);
        return Promise.reject(error);
      });
  } else {
    message.error(data.msg);
    return Promise.reject(error);
  }
};

// 请求拦截器
request.interceptors.request.use((config) => {
  const token = storage.get("Access-Token");
  // 如果 token 存在
  // 让每个请求携带自定义 token 请根据实际情况自行修改
  if (token) {
    config.headers["Authorization"] = "Bearer " + token;
  }
  return config;
}, errorHandler);

// 响应拦截器
request.interceptors.response.use((response) => {
  // 打印全局响应
  console.log(response.data);
  // 如果是文件下载类型的响应，直接返回
  if (response.config.responseType === 'blob') {
    return response;
  }
  if (response.data.status_code !== 200) {
    message.error(response.data.msg);
    return Promise.reject(response);
  }
  // message.success(response.data.msg);
  return response;
}, errorHandler);

export default request;
