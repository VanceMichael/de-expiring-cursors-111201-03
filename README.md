# 短效游标目录·完整采集

面向合作资料接收人员的 Python 采集组件。提供半开时间窗口的目录分页接口和短效游标，导入单页目录观察。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 可重启的目录扫描

供应商目录接口使用短效游标，翻页期间记录可能插入或修订，单窗可见数量有上限。
`scan` 命令把半开时间窗切成分片逐片翻页，状态落库，异常后可用同一命令续跑；
带事件时间线的输入（如 `fixtures/windows.json`，本地供应商分页脚本）经 `import`
入口也会走同一扫描流程，页原文与凭据同事务入库。

```bash
python3 -m collector scan --input fixtures/windows.json --db var/records.sqlite3
python3 -m collector coverage --db var/records.sqlite3
```

扫描报告给出窗口覆盖证明（叶子分片拼满扫描区间）、未完成区间和可靠水位；
水位前方所有区间完整时才前移。窗口被截断时按时间二分细分，到最小时间粒度
仍超上限的分片明确挂起，不自称完整；此时退出码为 3（完整封窗为 0，异常为 1）。
新扫描的起点按约定回看范围从水位回退，重复返回按供应商主键与修订号消化，
旧修订不会盖掉新值。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键：带修订号的记录仅在新修订更大时替换，旧修订与重复返回被消化；
无修订号的记录保持原有替换语义，不代表保留了完整修订历史。
`supplier.py` 把分页脚本模拟成本地目录来源；`scan.py` 执行可重启的分片扫描计划，
扫描、分片与水位状态保存在数据库的 scans / shards / watermark 表中。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入；
`fixtures/windows.json` 是本地供应商的分页脚本，驱动扫描演示与边界复现。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
