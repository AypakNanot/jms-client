package ui

import (
	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"
)

// tableModel 通用表格模型（消息列表 / 消费者组共用）。
type tableModel struct {
	walk.TableModelBase
	rows [][]string
}

func (m *tableModel) RowCount() int { return len(m.rows) }

func (m *tableModel) Value(row, col int) interface{} {
	if row < len(m.rows) && col < len(m.rows[row]) {
		return m.rows[row][col]
	}
	return ""
}

func (m *tableModel) reset(rows [][]string) {
	m.rows = rows
	m.PublishRowsReset()
}

// buildWelcome 空态欢迎卡（无连接时显示）。
func (a *app) buildWelcome() Composite {
	return Composite{
		AssignTo:      &a.welcome,
		StretchFactor: 1,
		Layout:        VBox{Spacing: 8},
		Children: []Widget{
			VSpacer{StretchFactor: 1},
			Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
				HSpacer{StretchFactor: 1},
				Label{
					Text:      "kafak-tools",
					Font:      Font{Family: "Microsoft YaHei", PointSize: 22, Bold: true},
					TextColor: walk.Color(0x0066CC),
				},
				HSpacer{StretchFactor: 1},
			}},
			Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
				HSpacer{StretchFactor: 1},
				Label{
					Text:      "Kafka / ActiveMQ 消息调试工具",
					Font:      Font{Family: "Microsoft YaHei", PointSize: 11},
					TextColor: walk.Color(0x666666),
				},
				HSpacer{StretchFactor: 1},
			}},
			Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
				HSpacer{StretchFactor: 1},
				PushButton{
					Text:    "＋ 新建连接 ...",
					MinSize: Size{Width: 200, Height: 42},
					Font:    Font{Family: "Microsoft YaHei", PointSize: 11},
					OnClicked: func() {
						a.newConn(a.mw)
					},
				},
				HSpacer{StretchFactor: 1},
			}},
			Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
				HSpacer{StretchFactor: 1},
				Label{
					Text:      "连接支持 Kafka 与 ActiveMQ(JMS)，可保存多套配置",
					TextColor: walk.Color(0x999999),
				},
				HSpacer{StretchFactor: 1},
			}},
			VSpacer{StretchFactor: 1},
		},
	}
}

// buildWorkArea 主工作区：标签页（消息浏览/发送/分区·组/概览）+ 消息详情。
func (a *app) buildWorkArea() Composite {
	return Composite{
		AssignTo:      &a.work,
		StretchFactor: 1,
		Layout:        VBox{Spacing: 6},
		Children: []Widget{
			TabWidget{
				AssignTo:      &a.tabs,
				StretchFactor: 3,
				Pages: []TabPage{
					// ---- 消息浏览 ----
					{
						Title:  "消息浏览",
						Layout: VBox{Spacing: 4},
						Children: []Widget{
							Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
								Label{Text: "主题:"},
								ComboBox{
									AssignTo:              &a.topicCB,
									Model:                 []string{"（先选择连接）"},
									StretchFactor:         1,
									OnCurrentIndexChanged: func() { a.syncSendTarget() },
								},
								Label{Text: "起点:"},
								ComboBox{
									AssignTo:              &a.startCB,
									Model:                 []string{"最新", "最早", "指定 offset"},
									MinSize:               Size{Width: 108, Height: 0},
									OnCurrentIndexChanged: func() { a.onStartModeChanged() },
								},
								LineEdit{AssignTo: &a.offsetEdit, Text: "0", MinSize: Size{Width: 90, Height: 0}, CueBanner: "offset"},
								Label{Text: "数量:"},
								LineEdit{AssignTo: &a.countEdit, Text: "100", MinSize: Size{Width: 60, Height: 0}},
								Label{Text: "过滤:"},
								LineEdit{AssignTo: &a.filterEdit, CueBanner: "文本/正则", MinSize: Size{Width: 150, Height: 0}},
								PushButton{
									AssignTo:  &a.queryBtn,
									Text:      " 查询",
									MinSize:   Size{Width: 84, Height: 0},
									OnClicked: func() { a.onQuery() },
								},
								PushButton{
									AssignTo:  &a.liveBtn,
									Text:      "实时订阅",
									MinSize:   Size{Width: 92, Height: 0},
									OnClicked: func() { a.toggleLive() },
								},
							}},
							TableView{
								AssignTo:              &a.msgTable,
								Model:                 a.msgModel,
								AlternatingRowBG:      true,
								StretchFactor:         3,
								OnCurrentIndexChanged: func() { a.renderDetail() },
								Columns: []TableViewColumn{
									{Title: "Offset", Width: 90},
									{Title: "分区", Width: 55},
									{Title: "时间", Width: 150},
									{Title: "Key", Width: 130},
									{Title: "预览", Width: 420},
								},
							},
							Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
								PushButton{
									AssignTo:  &a.prevBtn,
									Text:      "◀ 上一页",
									MinSize:   Size{Width: 92, Height: 0},
									OnClicked: func() { a.onPrevPage() },
								},
								PushButton{
									AssignTo:  &a.nextBtn,
									Text:      "下一页 ▶",
									MinSize:   Size{Width: 92, Height: 0},
									OnClicked: func() { a.onNextPage() },
								},
								Label{AssignTo: &a.pageInfo, Text: "", StretchFactor: 1},
							}},
							// ---- 消息详情（仅本标签页；只显示正文，字段以表格为准）----
							GroupBox{
								AssignTo:      &a.detailBox,
								Title:         "消息详情",
								StretchFactor: 1,
								MinSize:       Size{Width: 0, Height: 120},
								Layout:        VBox{Spacing: 4},
								Children: []Widget{
									Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
										PushButton{AssignTo: &a.copyBtn, Text: "复制", OnClicked: func() { a.onCopyDetail() }},
										PushButton{Text: "重发", OnClicked: func() { a.onResend() }},
										PushButton{Text: "删除", OnClicked: func() { a.onDeleteMsg() }},
										HSpacer{StretchFactor: 1},
										Label{Text: "格式:", TextColor: walk.Color(0x999999)},
										ComboBox{
											AssignTo:              &a.fmtCB,
											Model:                 []string{"原文", "美化 JSON", "Hex"},
											CurrentIndex:          0,
											MinSize:               Size{Width: 110, Height: 0},
											OnCurrentIndexChanged: func() { a.renderDetail() },
										},
									}},
									TextEdit{
										AssignTo:      &a.detailEdit,
										ReadOnly:      true,
										VScroll:       true,
										HScroll:       true,
										StretchFactor: 1,
										Text:          "在「消息浏览」中选择一条消息后，在此显示完整内容。",
									},
								},
							},
						},
					},
					// ---- 发送 ----
					{
						Title:  "发送消息",
						Layout: VBox{Spacing: 6},
						Children: []Widget{
							Composite{Layout: Grid{Columns: 2, Spacing: 6}, Children: []Widget{
								Label{Text: "目标 *:"},
								ComboBox{
									AssignTo: &a.targetCB,
									Editable: true,
									Model:    []string{"（先选择连接）"},
								},
								Label{Text: "分区(可空):"},
								LineEdit{AssignTo: &a.partitionEdit, CueBanner: "Kafka 专用，空=自动"},
								Label{Text: "Key(可空):"},
								LineEdit{AssignTo: &a.keyEdit, CueBanner: "Kafka 消息 key / JMS correlationId"},
							}},
							Composite{Layout: VBox{Spacing: 4}, Children: []Widget{
								Label{Text: "消息内容:"},
								TextEdit{
									AssignTo:      &a.sendBodyEdit,
									VScroll:       true,
									HScroll:       true,
									StretchFactor: 1,
									Text:          "{\n  \n}",
								},
							}},
							Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
								PushButton{
									AssignTo:  &a.sendBtn,
									Text:      "➤ 发送",
									MinSize:   Size{Width: 92, Height: 0},
									OnClicked: func() { a.onSend() },
								},
								Label{Text: "发送前请先在左侧选择连接", TextColor: walk.Color(0x999999)},
								HSpacer{StretchFactor: 1},
							}},
						},
					},
					// ---- 分区 / 消费者组 ----
					{
						Title:  "分区 / 消费者组",
						Layout: VBox{Spacing: 4},
						Children: []Widget{
							Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
								PushButton{
									Text:      "刷新",
									MinSize:   Size{Width: 72, Height: 0},
									OnClicked: func() { a.onRefreshGroups() },
								},
								Label{
									Text:          "按消费者组×分区展示消费进度：Lag = 未消费条数。工具的浏览不产生消费组——只有业务应用在消费该 Kafka 时这里才有数据。",
									TextColor:     walk.Color(0x666666),
									StretchFactor: 1,
								},
							}},
							TableView{
								AssignTo:         &a.grpTable,
								Model:            a.grpModel,
								AlternatingRowBG: true,
								StretchFactor:    1,
								Columns: []TableViewColumn{
									{Title: "消费者组", Width: 220},
									{Title: "主题", Width: 220},
									{Title: "分区", Width: 55},
									{Title: "当前 offset", Width: 110},
									{Title: "高水位", Width: 100},
									{Title: "Lag", Width: 80},
								},
							},
						},
					},
					// ---- 概览 ----
					{
						Title:  "概览",
						Layout: VBox{Spacing: 4},
						Children: []Widget{
							TextEdit{
								AssignTo:      &a.overviewEdit,
								ReadOnly:      true,
								VScroll:       true,
								StretchFactor: 1,
								Text:          "选择连接后，此处显示 broker / topic 概览（分区数、副本、配置）。",
							},
						},
					},
				},
			},
			// ---- 常驻消息框：操作响应日志（所有标签页可见）----
			GroupBox{
				Title:         "消息 · 操作日志",
				StretchFactor: 1,
				Layout:        VBox{Spacing: 4},
				Children: []Widget{
					TextEdit{
						AssignTo:      &a.logEdit,
						ReadOnly:      true,
						VScroll:       true,
						HScroll:       true,
						StretchFactor: 1,
						Text:          "就绪。操作响应显示在这里（保留最近 200 条）。",
					},
				},
			},
		},
	}
}
