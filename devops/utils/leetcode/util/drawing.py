import matplotlib
matplotlib.use('TkAgg')

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import fsolve

# 定义两个函数
def f1(n):
    return n ** 2

def f2(n):
    return 1 / n

# 定义差值函数，用于求交点
def diff(n):
    return f1(n) - f2(n)

# 近似交点的初始猜测
initial_guesses = [-1.5, 0.8]

# 求解交点
solutions = [fsolve(diff, guess)[0] for guess in initial_guesses]

# 去重、排序、四舍五入
points = [(round(n, 5), round(f1(n), 5)) for n in solutions]

# 构造绘图数据（跳过 n = 0 避免除 0）
n_left = np.linspace(-10, -0.1, 400)
n_right = np.linspace(0.1, 10, 400)
n_all = np.concatenate((n_left, n_right))

y1 = n_all ** 2
y2 = 1 / n_all

# 绘图
plt.figure(figsize=(8, 6))
plt.plot(n_all, y1, label='y = n²', color='purple')
plt.plot(n_all, y2, label='y = 1/n', color='green', linestyle='--')

# 标注交点
for x, y in points:
    plt.plot(x, y, 'ro')  # 红色圆点
    plt.text(x + 0.2, y, f'({x:.2f}, {y:.2f})', color='red')

# 图形修饰
plt.axhline(0, color='black', linewidth=0.5)
plt.axvline(0, color='black', linewidth=0.5)
plt.title('函数 y = n² 与 y = 1/n 的交点')
plt.xlabel('n')
plt.ylabel('y')
plt.legend()
plt.grid(True)
plt.ylim(-12, 12)
plt.xlim(-2.5, 2.5)
plt.show()
