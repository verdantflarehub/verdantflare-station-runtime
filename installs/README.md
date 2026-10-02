# Station Runtime 安装资产

`installs/` 是 `verdantflare-station-runtime` 唯一的安装入口。这里保存 Runtime 可复用的 Helm Chart、服务器初始化 Workflow 和本地校验/渲染脚本；不保存 Secret、模型权重、音视频资产或具体集群的临时事实。

目录结构：

```text
installs/
├── README.md
├── Workflow.md
├── helm/station-runtime/       # Runtime 可复用 Helm Chart
├── yaml/
│   └── etcd/                   # 单节点 etcd MCP 注册中心部署清单 (版本与 K8s 一致)
└── scripts/
    ├── server-init-workflow.sh # 服务器初始化门禁脚本，默认只读
    ├── validate-chart.sh       # Helm lint + template
    └── render-dev.sh           # 渲染审阅包，不执行 apply
```

## 两个仓库的职责

Runtime 仓库负责研发和版本化安装能力：Chart、初始化流程、探针契约、校验脚本和镜像发布。`verdantflare-design/deploys/k8s.dev.verdantflarehub.com/` 只保存 dev 环境实例化内容：节点、namespace、StorageClass/PV、ConfigMap、Secret 名称、probe Job 和验收证据。

## 晋级顺序

1. 在 Runtime 的 `dev` 分支修改 `installs/`，完成本地 Go/Helm/Shell 校验。
2. 使用 [Workflow.md](Workflow.md) 在 dev 服务器执行 S0–S8 门禁，所有命令显式指定 Kubernetes context。
3. 将验证通过的镜像版本、Chart 版本、模板摘要、probe Job 和 values 写入中央 dev 部署目录。
4. 只对中央目录执行声明式 apply，并保留回滚点和证据回执。

Chart 只部署 Runtime 自身，不安装 PostgreSQL、NVIDIA Device Plugin、StorageClass/PV 或业务推理服务；这些由 Workflow 和 dev 环境清单按阶段管理。
