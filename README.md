# 电台播出与版权窗口排程

一个不依赖第三方包、使用 SQLite 和标准库 HTTP 服务的电台排程项目。系统把“计划排期”和“实际播出”分开保存，支持地区授权、日期窗口、禁播时段、节目冷却、赞助商间隔、直播临时替换、实播对账与版权越界检查。

## 可恢复复核批次

授权、排期/替换、实播三处数据通过复核批次（review batch）串起来：

- **计划变更即作废重算**：每次排期或替换都会抬升全局 `plan_version`，当天进行中的批次（`open`）立即重算授权窗口、可播区间与对账结果。结果指纹未变的已确认项保留人工结论；指纹变化的项回退为待确认。
- **并发提交**：终确认用状态 CAS（`open → confirmed`），先到者胜出；后到者原样保存为 `draft` 草稿，并逐项记录与胜出结果的差异（verdict/note），草稿不能再次提交。
- **可恢复**：逐项确认各自提交，`last_item_seq` 记录连续确认到的检查点；写入失败自动重试（最多 3 次），重连后续做只处理仍 `pending` 的项，从最后确认项之后继续。
- **旧数据回填**：启动迁移时把没有批次号的历史对账异常归入 `legacy` 的初始版本批次（V01、已确认、确认人“系统回填”），之后发起复核自动递增版本号；旧批次保留为历史，不再被当成最新有效结果。

## 运行

需要 Python 3.11+。

```bash
python app.py
```

默认端口为 `8111`，页面地址是 <http://127.0.0.1:8111>。第一次启动会创建 `radio.db` 并写入三条演示排期。也可以设置端口和数据库位置：

```bash
PORT=9000 RADIO_DB=/tmp/radio.db python app.py
```

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖完整流程：排期、临时替换、播放日志、按日期对账；时间重叠、未授权地区和实播错节目等失败场景；以及批次重算与指纹保留、并发提交草稿与差异、写入失败重试与检查点续做、旧库回填与新版本升级。

## 主要 API

- `GET /api/state`：节目、每个排期的可播区间/阻断原因、全部复核批次、最近对账异常
- `POST /api/programs`：创建节目并授权地区
- `POST /api/programs/{id}/regions`：追加地区授权
- `GET /api/programs/{id}/window?date=&region=&start_time=`：查询授权窗口、当天可播区间与阻断原因
- `POST /api/schedule`：创建排期（计划版本 +1 并刷新进行中批次）
- `POST /api/slots/{id}/replace`：替换计划节目、重新校验并重算批次
- `POST /api/playout`：登记实播记录（进行中批次立即对账）
- `POST /api/reviews`：发起复核（同日已有 `open` 批次则幂等返回，断点续审）
- `POST /api/reviews/{id}/confirm`：确认下一个/指定条目（`{"seq":2}` 可省）
- `POST /api/reviews/{id}/submit`：确认全部并终提交；并发后到者返回 `outcome:"draft"` 及差异
- `GET /api/reviews?date=` / `GET /api/reviews/{id}`：批次列表与详情
- `POST /api/reconcile`：按日期对账（兼容旧入口，返回最新批次异常）

准备排期时填写 `air_date`、`start_time`、`program_id`、`region`。页面会直接显示校验错误，不会保存失败的排期；复核面板可发起复核、逐项确认、终提交，并展示每个节目的授权窗口、当天可播区间与阻断原因。
