// Package ui 实现 walk 桌面界面。
package ui

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"

	"kafak-tools/internal/adapter"
	"kafak-tools/internal/config"
)

// Run 启动主窗口。smoke=true 时建窗 1.5s 后自动关闭（冒烟测试）。
func Run(smoke, uitest bool) error {
	dir, err := exeDir()
	if err != nil {
		return err
	}
	store, loadErr := config.Load(dir)

	model := &connModel{}
	model.reset(store.Connections)

	a := &app{
		store:    store,
		model:    model,
		msgModel: &tableModel{},
		grpModel: &tableModel{},
	}

	var (
		mw *walk.MainWindow
		tv *walk.TreeView
	)

	if err := (MainWindow{
		AssignTo: &mw,
		Title:    "kafak-tools - 消息调试工具",
		MinSize:  Size{Width: 1100, Height: 680},
		Size:     Size{Width: 1280, Height: 820},
		Font:     Font{Family: "Microsoft YaHei", PointSize: 9},
		Layout:   VBox{Spacing: 4},
		StatusBarItems: []StatusBarItem{
			{Text: "就绪 · 配置目录: " + dir, Width: 560},
			{Text: "", Width: 420},
			{Text: "v1.0 · Kafka+ActiveMQ 全功能"},
		},
		ToolBar: ToolBar{
			// 默认 ToolBarButtonImageOnly 只渲染图标；无图标按钮需显式 TextOnly
			ButtonStyle: ToolBarButtonTextOnly,
			Items: []MenuItem{
				&Action{Text: "新建连接", OnTriggered: func() { a.newConn(mw) }},
				&Action{Text: "编辑", OnTriggered: func() { a.editConn(mw, tv) }},
				&Action{Text: "删除", OnTriggered: func() { a.removeConn(mw, tv) }},
				Separator{},
				&Action{Text: "刷新", OnTriggered: func() { a.reload(mw) }},
				&Action{Text: "发送消息", OnTriggered: func() { a.gotoSendTab() }},
			},
		},
		Children: []Widget{
			HSplitter{
				StretchFactor: 1,
				Children: []Widget{
					TreeView{
						AssignTo:             &tv,
						Model:                model,
						MinSize:              Size{Width: 150, Height: 0},
						OnItemActivated:      func() { a.editConn(mw, tv) },
						OnCurrentItemChanged: func() { a.onSelectConn() },
						StretchFactor:        1, // 与右侧 5 : 1 → 左栏约占 1/6
					},
					Composite{
						StretchFactor: 5,
						Layout:        VBox{Spacing: 0},
						Children: []Widget{
							a.buildWelcome(),
							a.buildWorkArea(),
						},
					},
				},
			},
		},
	}).Create(); err != nil {
		return fmt.Errorf("create main window: %w", err)
	}

	a.mw = mw
	a.tv = tv
	a.applyLayoutState()
	a.onStartModeChanged()

	if loadErr != nil {
		walk.MsgBox(mw, "配置读取失败", "已使用空配置启动：\n"+loadErr.Error(), walk.MsgBoxIconWarning)
	}

	if smoke {
		time.AfterFunc(1500*time.Millisecond, func() {
			mw.Synchronize(func() { _ = mw.Close() })
		})
	}
	if uitest {
		a.scheduleUITest()
	}

	// 消息循环内的 panic 会直接杀死进程（windowsgui 无处输出）——
	// 捕获后写 crash.log 并弹框，方便定位。
	defer func() {
		if r := recover(); r != nil {
			logCrash(dir, r, debug.Stack())
			walk.MsgBox(mw, "程序异常",
				fmt.Sprintf("发生内部错误：%v\n\n堆栈已写入:\n%s", r, filepath.Join(dir, "crash.log")),
				walk.MsgBoxIconError)
		}
	}()

	mw.Run()
	return nil
}

// scheduleUITest 自动化点击序列：复现按钮闪退（崩溃由 Run 的 recover 落盘）。
func (a *app) scheduleUITest() {
	step := func(delayMS int, f func()) {
		time.AfterFunc(time.Duration(delayMS)*time.Millisecond, func() {
			a.mw.Synchronize(f)
		})
	}
	step(700, func() { // 优先选中 Kafka 连接（uitest 步骤按 Kafka 设计）
		for i := 0; i < a.model.RootCount(); i++ {
			if ci, ok := a.model.RootAt(i).(*connItem); ok && ci.conn.Type == config.TypeKafka {
				_ = a.tv.SetCurrentItem(a.model.RootAt(i))
				return
			}
		}
		for i := 0; i < a.model.RootCount(); i++ {
			if it := a.model.RootAt(i); it != nil {
				_ = a.tv.SetCurrentItem(it)
				break
			}
		}
	})
	step(2000, func() { a.onQuery() }) // 最新 → 末页
	step(4200, func() { a.onNextPage() })
	step(5000, func() { a.onPrevPage() })
	step(5600, func() { a.reload(nil) }) // 工具栏刷新
	step(7400, func() {                  // 指定 offset 查询
		if a.startCB != nil {
			a.startCB.SetCurrentIndex(2)
		}
		a.onStartModeChanged()
		if a.offsetEdit != nil {
			a.offsetEdit.SetText("1")
		}
		a.onQuery()
	})
	step(9400, func() { // 回最早首页
		if a.startCB != nil {
			a.startCB.SetCurrentIndex(1)
		}
		a.onStartModeChanged()
		a.onQuery()
	})
	step(11800, func() {
		if a.msgTable != nil && a.msgModel.RowCount() > 0 {
			a.msgTable.SetCurrentIndex(0)
		}
	})
	step(12400, func() { a.onResend() })
	step(13200, func() { a.onCopyDetail() })
	step(13800, func() { // 切发送页并真实发送
		a.gotoSendTab()
		a.onSend()
	})
	step(15800, func() { a.toggleLive() }) // 实时订阅 开
	step(18800, func() { a.toggleLive() }) // 实时订阅 停
	step(20300, func() {
		_ = a.mw.Close()
	})
}

// logCrash 崩溃堆栈落盘（追加）。
func logCrash(dir string, r any, stack []byte) {
	f := filepath.Join(dir, "crash.log")
	_ = os.WriteFile(f,
		[]byte(fmt.Sprintf("=== panic %s ===\n%v\n%s\n", time.Now().Format(time.RFC3339), r, stack)),
		0o644)
}

// app 主窗口行为集合。
type app struct {
	mw *walk.MainWindow
	tv *walk.TreeView

	// 布局
	welcome *walk.Composite
	work    *walk.Composite
	tabs    *walk.TabWidget

	// 消息浏览
	msgTable      *walk.TableView
	topicCB       *walk.ComboBox
	startCB       *walk.ComboBox
	countEdit     *walk.LineEdit
	filterEdit    *walk.LineEdit
	queryBtn      *walk.PushButton
	liveBtn       *walk.PushButton
	msgModel      *tableModel
	topicNames    []string
	topicDepth    []int64
	lastMsgsTopic string // lastMsgs 的来源目的地（重发用）
	lastMsgs      []adapter.Message
	loadSeq       int

	// 发送
	targetCB      *walk.ComboBox
	partitionEdit *walk.LineEdit
	keyEdit       *walk.LineEdit
	sendBodyEdit  *walk.TextEdit
	sendBtn       *walk.PushButton

	// 分区/组
	grpTable *walk.TableView
	grpModel *tableModel

	// 概览与详情
	overviewEdit *walk.TextEdit
	detailEdit   *walk.TextEdit
	detailBox    *walk.GroupBox
	logEdit      *walk.TextEdit
	copyBtn      *walk.PushButton
	fmtCB        *walk.ComboBox

	// 连接数据
	store *config.Store
	model *connModel

	// 异步装载的中间结果（仅 UI 线程读写）
	pendingLoad struct {
		names    []string
		depths   []int64
		overview string
		rows     [][]string
	}
	pendingMsgs       []adapter.Message
	pendingRows       [][]string
	pendingSend       adapter.SendResult
	pendingSendTarget string
	liveStop          func() error
	liveSeq           int

	// 常规分页（页码制）
	offsetEdit           *walk.LineEdit
	prevBtn              *walk.PushButton
	nextBtn              *walk.PushButton
	pageInfo             *walk.Label
	pageTopic            string
	pageable             bool
	pageBase             int64 // 页 1 的起始 offset（窗口起点）
	pageNo               int
	pageCount            int
	totalMsgs            int64
	pageSize             int
	pageFilter           string
	pendingRestoreTarget string
	pendingState         struct {
		base, total     int64
		no, count, size int
		filter, topic   string
	}
}

// ---- 后台执行 ----

// async 后台执行 work，完成后回 UI 线程执行 done。
func (a *app) async(work func() error, done func(error)) {
	go func() {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					logCrash(exeDirMust(), r, debug.Stack())
					err = fmt.Errorf("内部异常: %v", r)
				}
			}()
			err = work()
		}()
		a.mw.Synchronize(func() { done(err) })
	}()
}

// exeDirMust 尽力取 exe 目录（崩溃路径用，失败返回当前目录）。
func exeDirMust() string {
	if d, err := exeDir(); err == nil {
		return d
	}
	return "."
}

// statusMsg 成功类反馈：状态栏 + 常驻消息框双写（带时间戳，不出弹窗、无提示音）。
// 错误/警告类仍用 MsgBox 弹窗打断。调用方须在 UI 线程。
func (a *app) statusMsg(text string) {
	defer func() { _ = recover() }() // 状态栏异常绝不允许把程序带崩
	a.logMsg(text)
	if a.mw == nil {
		return
	}
	sb := a.mw.StatusBar()
	if sb == nil {
		return
	}
	if it := sb.Items().At(1); it != nil {
		_ = it.SetText(time.Now().Format("15:04:05") + "  " + text)
	}
}

// setMultiline 写入多行文本（Win32 EDIT 控件只认 CRLF，统一将 LF 转 CRLF）。
func setMultiline(te *walk.TextEdit, text string) {
	if te == nil {
		return
	}
	t := strings.ReplaceAll(text, "\r\n", "\n")
	t = strings.ReplaceAll(t, "\n", "\r\n")
	_ = te.SetText(t)
}

// normalizeLF 把 CRLF 归一为 LF（拼接前用）。
func normalizeLF(s string) string {
	return strings.ReplaceAll(s, "\r\n", "\n")
}

const logMaxLines = 200

// logMsg 常驻消息框追加一行（带时间戳，保留最近 200 条）。
func (a *app) logMsg(text string) {
	if a.logEdit == nil {
		return
	}
	nl := "\n"
	line := time.Now().Format("15:04:05") + "  " + text
	cur := normalizeLF(a.logEdit.Text())
	out := line
	if cur != "" && !strings.HasSuffix(cur, "（保留最近 200 条）。") {
		out = cur + nl + line
	}
	if parts := strings.Split(out, nl); len(parts) > logMaxLines {
		out = strings.Join(parts[len(parts)-logMaxLines:], nl)
	}
	setMultiline(a.logEdit, out)
}

// ---- 连接选中 → 装载 topics / groups / 概览 ----

func (a *app) onSelectConn() {
	if a.mw == nil || a.topicCB == nil {
		return
	}
	if _, ok := currentItem(a.tv); !ok {
		a.clearBrowse("（先选择连接）")
		return
	}
	a.stopLiveOnSwitch()
	a.loadConn(false)
}

// loadConn 连接选中/发送后刷新目的地。
// keepMsgs=true：仅刷新主题列表与概览，不清空已浏览的消息与分页（发送成功后用）。
func (a *app) loadConn(keepMsgs bool) {
	item, ok := currentItem(a.tv)
	if !ok {
		if !keepMsgs {
			a.clearBrowse("（先选择连接）")
		}
		return
	}
	conn := item.conn
	a.loadSeq++
	seq := a.loadSeq

	_ = a.topicCB.SetModel([]string{"加载中..."})
	a.topicCB.SetCurrentIndex(0)
	brokerRef := conn.Kafka.Bootstrap
	if conn.Type == config.TypeActiveMQ {
		brokerRef = conn.ActiveMQ.JolokiaURL
	}
	if !keepMsgs {
		setMultiline(a.overviewEdit, "连接中: "+brokerRef+" ...")
	}

	a.async(func() error {
		ad, err := adapter.New(conn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		topics, err := ad.ListTopics(ctx)
		if err != nil {
			return err
		}
		var groups []adapter.GroupRow
		if conn.Type == config.TypeKafka {
			gs, gerr := ad.ListGroups(ctx)
			if gerr == nil {
				groups = gs
			}
		}
		names := make([]string, 0, len(topics))
		depths := make([]int64, 0, len(topics))
		var ov strings.Builder
		fmt.Fprintf(&ov, "连接: %s\n类型: %s\nBroker: %s\n目的地数: %d\n\n",
			conn.Name, conn.Type, brokerRef, len(topics))
		for _, t := range topics {
			names = append(names, t.Name)
			depths = append(depths, t.Depth)
			if conn.Type == config.TypeActiveMQ {
				if t.Depth >= 0 {
					fmt.Fprintf(&ov, "  %s  (积压 %d)\n", t.Name, t.Depth)
				} else {
					fmt.Fprintf(&ov, "  %s  (topic)\n", t.Name)
				}
			} else {
				fmt.Fprintf(&ov, "  %s  (%d 分区)\n", t.Name, t.Partitions)
			}
		}
		if len(names) == 0 {
			names = []string{"（无 topic）"}
		}
		var rows [][]string
		for _, g := range groups {
			rows = append(rows, []string{
				g.Group, g.Topic,
				strconv.Itoa(int(g.Partition)),
				itoa(g.Committed), itoa(g.End), itoa(g.Lag),
			})
		}
		a.pendingLoad.names = names
		a.pendingLoad.depths = depths
		a.pendingLoad.overview = ov.String()
		a.pendingLoad.rows = rows
		return nil
	}, func(err error) {
		restoreTarget := a.pendingRestoreTarget
		a.pendingRestoreTarget = ""
		if seq != a.loadSeq {
			return // 连接已切换，丢弃过期结果
		}
		if err != nil {
			a.clearBrowse("（加载失败）")
			walk.MsgBox(a.mw, "连接失败", err.Error(), walk.MsgBoxIconError)
			a.logMsg("连接失败: " + err.Error())
			return
		}
		// 刷新前选中的主题，刷新后尽量保持
		prevTopic := ""
		if idx := a.topicCB.CurrentIndex(); idx >= 0 && idx < len(a.topicNames) {
			prevTopic = a.topicNames[idx]
		}
		a.topicNames = a.pendingLoad.names
		a.topicDepth = a.pendingLoad.depths
		_ = a.topicCB.SetModel(a.pendingLoad.names)
		sel := 0
		if prevTopic != "" {
			for i, n := range a.topicNames {
				if n == prevTopic {
					sel = i
					break
				}
			}
		}
		a.topicCB.SetCurrentIndex(sel)
		setMultiline(a.overviewEdit, a.pendingLoad.overview)
		a.grpModel.reset(a.pendingLoad.rows)
		if !keepMsgs {
			a.clearDetail()
			a.msgModel.reset(nil)
			a.lastMsgs = nil
			a.pageTopic = ""
			a.pageable = false
			a.pageBase = 0
			a.pageNo = 0
			a.pageCount = 0
			a.totalMsgs = 0
			if a.pageInfo != nil {
				a.pageInfo.SetText("")
			}
		}
		// 发送目标下拉：带出全部可选目的地，默认选中浏览页当前主题
		if a.targetCB != nil {
			if names := a.sendTargetNames(); len(names) > 0 {
				_ = a.targetCB.SetModel(names)
				a.targetCB.SetText(names[0])
			} else {
				_ = a.targetCB.SetModel([]string{"（无主题）"})
			}
			a.syncSendTarget()
		}
		if restoreTarget != "" && a.targetCB != nil {
			a.targetCB.SetText(restoreTarget)
		}
		a.logMsg(fmt.Sprintf("已加载「%s」: %d 个目的地", conn.Name, len(a.topicNames)))
		a.updatePageButtons()
	})
}

func (a *app) clearBrowse(hint string) {
	a.stopLiveOnSwitch()
	a.topicNames = nil
	a.topicDepth = nil
	_ = a.topicCB.SetModel([]string{hint})
	a.topicCB.SetCurrentIndex(0)
	if a.targetCB != nil {
		_ = a.targetCB.SetModel([]string{hint})
	}
	a.msgModel.reset(nil)
	a.lastMsgs = nil
	a.pageTopic = ""
	a.pageable = false
	a.pageBase = 0
	a.pageNo = 0
	a.pageCount = 0
	a.totalMsgs = 0
	if a.pageInfo != nil {
		a.pageInfo.SetText("")
	}
	a.updatePageButtons()
	a.clearDetail()
}

// ---- 查询 ----

// onQuery 按当前选择真实拉取消息（零副作用）。
// onQuery 首查：解析条件 → 计算总页数/总数 → 取第一页。
func (a *app) onQuery() {
	item, ok := currentItem(a.tv)
	if !ok {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择一个连接。", walk.MsgBoxIconInformation)
		return
	}
	if a.topicCB == nil || len(a.topicNames) == 0 || a.topicCB.CurrentIndex() < 0 ||
		strings.HasPrefix(a.topicNames[a.topicCB.CurrentIndex()], "（") {
		walk.MsgBox(a.mw, "提示", "没有可查询的主题。", walk.MsgBoxIconInformation)
		return
	}
	topic := a.topicNames[a.topicCB.CurrentIndex()]
	size := 100
	if n, err := strconv.Atoi(strings.TrimSpace(a.countEdit.Text())); err == nil && n > 0 {
		if n > 1000 {
			n = 1000
		}
		size = n
	}
	mode := "latest"
	if a.startCB != nil {
		switch a.startCB.CurrentIndex() {
		case 1:
			mode = "earliest"
		case 2:
			mode = "offset"
		}
	}
	var offsetVal int64
	if mode == "offset" {
		v, err := strconv.ParseInt(strings.TrimSpace(a.offsetEdit.Text()), 10, 64)
		if err != nil || v < 0 {
			walk.MsgBox(a.mw, "校验", "offset 必须是非负整数。", walk.MsgBoxIconWarning)
			return
		}
		offsetVal = v
	}
	a.pageTopic = topic
	a.pageable = item.conn.Type == config.TypeKafka
	a.fetchPage(topic, mode, offsetVal, size, strings.TrimSpace(a.filterEdit.Text()), 0)
}

// onPrevPage / onNextPage 页码制翻页（仅 Kafka；边界由按钮灰态拦住）。
func (a *app) onPrevPage() {
	if a.pageNo <= 1 {
		a.statusMsg("已是第一页")
		return
	}
	a.fetchPage(a.pageTopic, "", 0, a.pageSize, a.pageFilter, a.pageNo-1)
}

func (a *app) onNextPage() {
	if a.pageCount <= 0 || a.pageNo >= a.pageCount {
		a.statusMsg("已是最后一页")
		return
	}
	a.fetchPage(a.pageTopic, "", 0, a.pageSize, a.pageFilter, a.pageNo+1)
}

// updatePageButtons 只在真正边界置灰：第 1 页禁上一页，最后一页禁下一页。
func (a *app) updatePageButtons() {
	if a.prevBtn == nil || a.nextBtn == nil {
		return
	}
	if a.liveStop != nil || !a.pageable || a.pageCount <= 0 {
		a.prevBtn.SetEnabled(false)
		a.nextBtn.SetEnabled(false)
		return
	}
	a.prevBtn.SetEnabled(a.pageNo > 1)
	a.nextBtn.SetEnabled(a.pageNo < a.pageCount)
}

// onStartModeChanged 「指定 offset」时显示 offset 输入框。
func (a *app) onStartModeChanged() {
	if a.offsetEdit == nil || a.startCB == nil {
		return
	}
	a.offsetEdit.SetVisible(a.startCB.CurrentIndex() == 2)
}

// fetchPage 取某一页。targetPage=0 表示首查（先 Bounds 算总数/页数）；
// >0 为翻页（复用已存窗口状态，值在调用时捕获，避免后台读 UI 字段的竞态）。
func (a *app) fetchPage(topic, mode string, offsetVal int64, size int, filter string, targetPage int) {
	item, ok := currentItem(a.tv)
	if !ok {
		return
	}
	conn := item.conn
	capBase := a.pageBase
	a.queryBtn.SetEnabled(false)
	a.updatePageButtons()

	a.async(func() error {
		ad, err := adapter.New(conn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		pageNo := targetPage
		base := capBase
		count := a.pageCount
		total := a.totalMsgs
		if targetPage == 0 {
			st, tot, err := ad.Bounds(ctx, topic)
			if err != nil {
				return err
			}
			base = st
			end := st + tot
			switch mode {
			case "offset":
				if offsetVal > base {
					base = offsetVal
				}
			case "latest":
				// 稍后按 count 定位最后一页
			}
			avail := end - base
			if avail < 0 {
				avail = 0
			}
			count = int((avail + int64(size) - 1) / int64(size))
			if count < 1 {
				count = 1
			}
			total = avail
			pageNo = 1
			if mode == "latest" {
				pageNo = count // 最新 = 跳到最后一页
			}
		}
		pageStart := base + int64(pageNo-1)*int64(size)
		msgs, err := ad.Browse(ctx, topic, adapter.BrowseOptions{
			Start: "offset", Offset: pageStart, Limit: size, Filter: filter,
		})
		if err != nil {
			return err
		}
		rows := make([][]string, 0, len(msgs))
		for _, m := range msgs {
			rows = append(rows, []string{
				strconv.FormatInt(m.Offset, 10),
				strconv.Itoa(int(m.Partition)),
				m.Time.Format("2006-01-02 15:04:05"),
				truncate(m.Key, 60),
				truncate(strings.ReplaceAll(m.Value, "\n", " "), 120),
			})
		}
		a.pendingMsgs = msgs
		a.pendingRows = rows
		a.pendingState.base = base
		a.pendingState.total = total
		a.pendingState.no = pageNo
		a.pendingState.count = count
		a.pendingState.size = size
		a.pendingState.filter = filter
		a.pendingState.topic = topic
		return nil
	}, func(err error) {
		a.queryBtn.SetEnabled(true)
		if err != nil {
			walk.MsgBox(a.mw, "查询失败", err.Error(), walk.MsgBoxIconError)
			a.logMsg("查询失败: " + err.Error())
			a.updatePageButtons()
			return
		}
		a.lastMsgs = a.pendingMsgs
		a.lastMsgsTopic = topic
		a.pageBase = a.pendingState.base
		a.totalMsgs = a.pendingState.total
		a.pageNo = a.pendingState.no
		a.pageCount = a.pendingState.count
		a.pageSize = a.pendingState.size
		a.pageFilter = a.pendingState.filter
		a.msgModel.reset(a.pendingRows)
		a.msgTable.SetCurrentIndex(-1)
		a.logMsg(fmt.Sprintf("查询完成: %d 条 · %s", len(a.pendingRows), topic))
		if a.pageInfo != nil {
			if !a.pageable {
				a.pageInfo.SetText(fmt.Sprintf("共 %d 条 · 队列浏览不分页", a.totalMsgs))
			} else {
				a.pageInfo.SetText(fmt.Sprintf("第 %d / %d 页 · 共 %d 条 · 每页 %d",
					a.pageNo, a.pageCount, a.totalMsgs, a.pageSize))
			}
		}
		if len(a.pendingRows) == 0 {
			a.detailEdit.SetText("没有匹配的消息。")
		} else {
			a.clearDetail()
		}
		a.updatePageButtons()
	})
}

// renderDetail 按所选格式渲染当前消息。
// renderDetail 只显示消息正文（字段以表格为准，默认原文）。
// 格式下拉：0=原文 1=美化 JSON 2=Hex
func (a *app) renderDetail() {
	if a.detailEdit == nil || a.msgTable == nil {
		return
	}
	idx := a.msgTable.CurrentIndex()
	if idx < 0 || idx >= len(a.lastMsgs) {
		return
	}
	m := a.lastMsgs[idx]
	setDetailTitle := func(nonJSON bool) {
		if a.detailBox == nil {
			return
		}
		if nonJSON {
			_ = a.detailBox.SetTitle("消息详情 · 非 JSON（已按原文显示）")
		} else {
			_ = a.detailBox.SetTitle("消息详情")
		}
	}
	switch {
	case a.fmtCB != nil && a.fmtCB.CurrentIndex() == 1: // 美化 JSON
		pv := prettyJSON(m.Value)
		setDetailTitle(pv == m.Value) // 透传 = 原消息不是 JSON
		setMultiline(a.detailEdit, pv)
	case a.fmtCB != nil && a.fmtCB.CurrentIndex() == 2: // Hex
		setDetailTitle(false)
		var b strings.Builder
		data := []byte(m.Value)
		for i, c := range data {
			if i > 0 && i%16 == 0 {
				b.WriteString("\n")
			} else if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(hex.EncodeToString([]byte{c}))
		}
		setMultiline(a.detailEdit, b.String())
	default: // 原文（默认）
		setDetailTitle(false)
		setMultiline(a.detailEdit, m.Value)
	}
}

func (a *app) clearDetail() {
	if a.detailEdit != nil {
		a.detailEdit.SetText("在「消息浏览」中选择一条消息后，在此显示完整内容。")
	}
}

// prettyJSON 若是合法 JSON 则美化，否则原样返回。
func prettyJSON(s string) string {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return s
	}
	return string(b)
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// ---- 工具栏 / 标签页行为 ----

// onRefreshGroups 重新拉取目的地与消费者组（保留已浏览的消息）。
func (a *app) onRefreshGroups() {
	a.loadConn(true)
	a.statusMsg("已刷新目的地与消费者组")
}

func (a *app) gotoSendTab() {
	if a.tabs != nil {
		a.tabs.SetCurrentIndex(1)
	}
}

// sendTargetNames 过滤掉占位符后的可选目的地列表。
func (a *app) sendTargetNames() []string {
	var out []string
	for _, n := range a.topicNames {
		if !strings.HasPrefix(n, "（") {
			out = append(out, n)
		}
	}
	return out
}

// syncSendTarget 把浏览页选中的主题同步为发送目标（可编辑，用户仍可手改/新输）。
func (a *app) syncSendTarget() {
	if a.targetCB == nil || a.topicCB == nil {
		return
	}
	idx := a.topicCB.CurrentIndex()
	if idx >= 0 && idx < len(a.topicNames) && !strings.HasPrefix(a.topicNames[idx], "（") {
		a.targetCB.SetText(a.topicNames[idx])
	}
}

func (a *app) onSend() {
	item, ok := currentItem(a.tv)
	if !ok {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择一个连接。", walk.MsgBoxIconInformation)
		return
	}
	conn := item.conn
	target := ""
	if a.targetCB != nil {
		target = strings.TrimSpace(a.targetCB.Text())
		if target == "" {
			names := a.sendTargetNames()
			if idx := a.targetCB.CurrentIndex(); idx >= 0 && idx < len(names) {
				target = names[idx]
			}
		}
	}
	if target == "" {
		walk.MsgBox(a.mw, "校验", "请选择或输入目标（Kafka=topic，ActiveMQ=queue/topic 名）。", walk.MsgBoxIconWarning)
		return
	}
	partition := int32(-1)
	if p := strings.TrimSpace(a.partitionEdit.Text()); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			walk.MsgBox(a.mw, "校验", "分区号必须是非负整数，留空=自动分区。", walk.MsgBoxIconWarning)
			return
		}
		partition = int32(n)
	}
	value := a.sendBodyEdit.Text()
	key := a.keyEdit.Text()
	a.sendBtn.SetEnabled(false)

	a.async(func() error {
		ad, err := adapter.New(conn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, err := ad.Send(ctx, adapter.SendOptions{
			Target: target, Partition: partition, Key: key, Value: value,
		})
		if err != nil {
			return err
		}
		a.pendingSend = res
		a.pendingSendTarget = target
		return nil
	}, func(err error) {
		a.sendBtn.SetEnabled(true)
		if err != nil {
			walk.MsgBox(a.mw, "发送失败", err.Error(), walk.MsgBoxIconError)
			a.logMsg("发送失败: " + err.Error())
			return
		}
		if a.pendingSend.MsgID != "" {
			a.statusMsg(fmt.Sprintf("发送成功 → %s，消息 ID %s", a.pendingSendTarget, a.pendingSend.MsgID))
			return
		}
		a.statusMsg(fmt.Sprintf("发送成功 → %s（分区 %d，偏移 %d）", a.pendingSendTarget, a.pendingSend.Partition, a.pendingSend.Offset))

		if a.liveStop == nil {
			a.pendingRestoreTarget = target
			a.loadConn(true)
		}
	})
}

func (a *app) onCopyDetail() {
	if a.detailEdit == nil || a.detailEdit.Text() == "" {
		return
	}
	if err := walk.Clipboard().SetText(a.detailEdit.Text()); err != nil {
		walk.MsgBox(a.mw, "复制失败", err.Error(), walk.MsgBoxIconError)
		return
	}
	a.statusMsg("已复制到剪贴板")
}

// onResend 将选中消息按原目的地一键重发（Kafka 保持原分区与 Key）。
func (a *app) onResend() {
	item, ok := currentItem(a.tv)
	if !ok {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择一个连接。", walk.MsgBoxIconInformation)
		return
	}
	idx := a.msgTable.CurrentIndex()
	if idx < 0 || idx >= len(a.lastMsgs) {
		walk.MsgBox(a.mw, "提示", "请先在消息列表中选择一条消息。", walk.MsgBoxIconInformation)
		return
	}
	if a.lastMsgsTopic == "" {
		walk.MsgBox(a.mw, "提示", "无法确定消息来源目的地，请重新查询。", walk.MsgBoxIconInformation)
		return
	}
	conn := item.conn
	m := a.lastMsgs[idx]

	a.async(func() error {
		ad, err := adapter.New(conn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		partition := int32(-1)
		if conn.Type == config.TypeKafka {
			partition = m.Partition // 原样重发：保持分区
		}
		res, err := ad.Send(ctx, adapter.SendOptions{
			Target: a.lastMsgsTopic, Partition: partition, Key: m.Key, Value: m.Value,
		})
		if err != nil {
			return err
		}
		a.pendingSend = res
		a.pendingSendTarget = a.lastMsgsTopic
		return nil
	}, func(err error) {
		if err != nil {
			walk.MsgBox(a.mw, "重发失败", err.Error(), walk.MsgBoxIconError)
			a.logMsg("重发失败: " + err.Error())
			return
		}
		if a.pendingSend.MsgID != "" {
			a.statusMsg(fmt.Sprintf("重发成功 → %s，消息 ID %s", a.pendingSendTarget, a.pendingSend.MsgID))
			return
		}
		a.statusMsg(fmt.Sprintf("重发成功 → %s（分区 %d，偏移 %d）", a.pendingSendTarget, a.pendingSend.Partition, a.pendingSend.Offset))
	})
}

func (a *app) onDeleteMsg() {
	item, ok := currentItem(a.tv)
	if !ok {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择一个连接。", walk.MsgBoxIconInformation)
		return
	}
	if item.conn.Type == config.TypeKafka {
		walk.MsgBox(a.mw, "提示",
			"Kafka 语义不支持删除单条消息；如需清理请调整 retention 或删除 topic。",
			walk.MsgBoxIconInformation)
		return
	}
	idx := -1
	if a.topicCB != nil {
		idx = a.topicCB.CurrentIndex()
	}
	if idx < 0 || idx >= len(a.topicNames) || strings.HasPrefix(a.topicNames[idx], "（") {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择连接并在主题下拉中选择队列。", walk.MsgBoxIconInformation)
		return
	}
	dest := a.topicNames[idx]
	if idx < len(a.topicDepth) && a.topicDepth[idx] < 0 {
		walk.MsgBox(a.mw, "提示", dest+" 是 topic（无存储），无法清空；队列才可清空。", walk.MsgBoxIconInformation)
		return
	}
	ans := walk.MsgBox(a.mw, "清空队列",
		fmt.Sprintf("确认清空队列「%s」的全部消息？\n\n此操作不可恢复！", dest),
		walk.MsgBoxYesNo|walk.MsgBoxIconExclamation)
	if ans != walk.DlgCmdYes {
		return
	}
	conn := item.conn
	a.async(func() error {
		ad, err := adapter.New(conn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return ad.Purge(ctx, dest)
	}, func(err error) {
		if err != nil {
			walk.MsgBox(a.mw, "清空失败", err.Error(), walk.MsgBoxIconError)
			a.logMsg("清空失败: " + err.Error())
			return
		}
		a.msgModel.reset(nil)
		a.lastMsgs = nil
		a.clearDetail()
		a.statusMsg("队列「" + dest + "」已清空")
	})
}

// ---- 实时订阅（STOMP，topic 专用） ----

// stopLiveOnSwitch 连接/主题切换时停掉订阅（幂等）。
func (a *app) stopLiveOnSwitch() {
	if a.liveStop != nil {
		_ = a.liveStop()
		a.liveStop = nil
		if a.liveBtn != nil {
			a.liveBtn.SetText("实时订阅")
		}
		if a.pageInfo != nil {
			a.pageInfo.SetText("")
		}
		a.updatePageButtons()
	}
}

// toggleLive 启停 STOMP 实时订阅。
func (a *app) toggleLive() {
	if a.liveStop != nil {
		a.stopLiveOnSwitch()
		return
	}
	item, ok := currentItem(a.tv)
	if !ok {
		walk.MsgBox(a.mw, "提示", "请先在左侧选择一个连接。", walk.MsgBoxIconInformation)
		return
	}
	conn := item.conn

	idx := -1
	if a.topicCB != nil {
		idx = a.topicCB.CurrentIndex()
	}
	if idx < 0 || idx >= len(a.topicNames) || strings.HasPrefix(a.topicNames[idx], "（") {
		walk.MsgBox(a.mw, "提示", "请先选择主题。", walk.MsgBoxIconInformation)
		return
	}
	dest := a.topicNames[idx]

	// topicDepth >= 0 表示 ActiveMQ 队列（Kafka 恒为 -1）：STOMP 订阅队列会真正消费消息，禁止
	if idx < len(a.topicDepth) && a.topicDepth[idx] >= 0 {
		walk.MsgBox(a.mw, "提示",
			dest+" 是队列：STOMP 订阅队列会真正消费消息（有副作用），请用「查询」刷新浏览。",
			walk.MsgBoxIconInformation)
		return
	}
	if conn.Type == config.TypeActiveMQ {
		if !conn.ActiveMQ.STOMPEnabled || conn.ActiveMQ.STOMPAddr == "" {
			walk.MsgBox(a.mw, "STOMP 未启用",
				"请编辑该连接，勾选「启用 STOMP」并填写地址。\n\n注意：服务端 activemq.xml 需启用 stomp 连接器（参考环境默认注释，需取消注释并重启）。",
				walk.MsgBoxIconWarning)
			return
		}
	}

	a.liveSeq++
	seq := a.liveSeq
	isKafkaLive := conn.Type == config.TypeKafka
	push := func(m adapter.Message) {
		a.mw.Synchronize(func() {
			if seq != a.liveSeq {
				return
			}
			if !isKafkaLive {
				// AMQ 无原生 offset，用位置序号；Kafka 保留真实 offset
				m.Offset = int64(len(a.lastMsgs))
			}
			a.lastMsgs = append(a.lastMsgs, m)
			a.msgModel.reset(a.msgRows())
		})
	}
	fail := func(err error) {
		a.mw.Synchronize(func() {
			if seq != a.liveSeq {
				return
			}
			a.stopLiveOnSwitch()
			walk.MsgBox(a.mw, "订阅中断", err.Error(), walk.MsgBoxIconWarning)
			a.logMsg("订阅中断: " + err.Error())
		})
	}

	var stop func() error
	var err error
	if conn.Type == config.TypeKafka {
		stop, err = adapter.SubscribeKafkaLatest(conn.Kafka, dest, push, fail)
	} else {
		stop, err = adapter.SubscribeTopic(conn.ActiveMQ, dest, push, fail)
	}
	if err != nil {
		walk.MsgBox(a.mw, "订阅失败", err.Error(), walk.MsgBoxIconError)
		a.logMsg("订阅失败: " + err.Error())
		return
	}
	a.liveStop = stop
	a.liveBtn.SetText("停止订阅")
	// 保留已有查询结果：同主题直接追加；跨主题才重置列表
	if a.lastMsgsTopic != "" && a.lastMsgsTopic != dest {
		a.msgModel.reset(nil)
		a.lastMsgs = nil
	}
	a.lastMsgsTopic = dest
	if a.pageInfo != nil {
		a.pageInfo.SetText("实时接收中…（已保留之前的查询结果）")
	}
	a.updatePageButtons()
	a.statusMsg("已订阅「" + dest + "」，新消息将追加显示…（再点一次停止）")
}

// msgRows 由 lastMsgs 生成表格行。
func (a *app) msgRows() [][]string {
	rows := make([][]string, 0, len(a.lastMsgs))
	for _, m := range a.lastMsgs {
		rows = append(rows, []string{
			strconv.FormatInt(m.Offset, 10),
			strconv.Itoa(int(m.Partition)),
			m.Time.Format("2006-01-02 15:04:05"),
			truncate(m.Key, 60),
			truncate(strings.ReplaceAll(m.Value, "\n", " "), 120),
		})
	}
	return rows
}

// ---- 连接管理 ----

func (a *app) newConn(owner walk.Form) {
	if _, err := openConnDialog(owner, a.store, nil); err != nil {
		walk.MsgBox(owner, "错误", err.Error(), walk.MsgBoxIconError)
		return
	}
	a.refreshTree()
}

func (a *app) editConn(owner walk.Form, tv *walk.TreeView) {
	item, ok := currentItem(tv)
	if !ok {
		walk.MsgBox(owner, "提示", "请先在左侧选择一个连接", walk.MsgBoxIconInformation)
		return
	}
	conn, ok := a.store.Get(item.conn.ID)
	if !ok {
		a.refreshTree()
		return
	}
	if _, err := openConnDialog(owner, a.store, &conn); err != nil {
		walk.MsgBox(owner, "错误", err.Error(), walk.MsgBoxIconError)
		return
	}
	a.refreshTree()
}

func (a *app) removeConn(owner walk.Form, tv *walk.TreeView) {
	item, ok := currentItem(tv)
	if !ok {
		walk.MsgBox(owner, "提示", "请先在左侧选择一个连接", walk.MsgBoxIconInformation)
		return
	}
	ans := walk.MsgBox(owner, "删除连接", fmt.Sprintf("确认删除连接「%s」？", item.conn.Name),
		walk.MsgBoxYesNo|walk.MsgBoxIconQuestion)
	if ans != walk.DlgCmdYes {
		return
	}
	a.store.Remove(item.conn.ID)
	if err := a.store.Save(); err != nil {
		walk.MsgBox(owner, "保存失败", err.Error(), walk.MsgBoxIconError)
		return
	}
	a.refreshTree()
}

func (a *app) reload(owner walk.Form) {
	dir, err := exeDir()
	if err != nil {
		walk.MsgBox(owner, "错误", err.Error(), walk.MsgBoxIconError)
		return
	}
	s, err := config.Load(dir)
	if err != nil {
		walk.MsgBox(owner, "配置读取失败", err.Error(), walk.MsgBoxIconError)
		return
	}
	a.store = s
	a.refreshTree()
}

func (a *app) refreshTree() {
	a.model.reset(a.store.Connections)
	a.applyLayoutState()
	a.onSelectConn()
}

// applyLayoutState 无连接 → 欢迎卡；有连接 → 工作区。
func (a *app) applyLayoutState() {
	if a.welcome == nil || a.work == nil {
		return
	}
	empty := len(a.store.Connections) == 0
	a.welcome.SetVisible(empty)
	a.work.SetVisible(!empty)
}

// ---- 连接树模型 ----

type connItem struct {
	conn config.Connection
}

func (i *connItem) Text() string {
	switch i.conn.Type {
	case config.TypeKafka:
		return "[Kafka] " + i.conn.Name
	case config.TypeActiveMQ:
		return "[ActiveMQ] " + i.conn.Name
	}
	return i.conn.Name
}

func (i *connItem) Parent() walk.TreeItem     { return nil }
func (i *connItem) ChildCount() int           { return 0 }
func (i *connItem) ChildAt(int) walk.TreeItem { return nil }

type connModel struct {
	walk.TreeModelBase
	items []*connItem
}

func (m *connModel) reset(conns []config.Connection) {
	m.items = make([]*connItem, 0, len(conns))
	for _, c := range conns {
		m.items = append(m.items, &connItem{conn: c})
	}
	m.PublishItemsReset(nil)
}

func (m *connModel) RootCount() int             { return len(m.items) }
func (m *connModel) RootAt(i int) walk.TreeItem { return m.items[i] }

// ---- 工具 ----

func currentItem(tv *walk.TreeView) (*connItem, bool) {
	if tv == nil {
		return nil, false
	}
	it, ok := tv.CurrentItem().(*connItem)
	return it, ok
}

func exeDir() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return os.Getwd()
	}
	return filepath.Dir(p), nil
}
