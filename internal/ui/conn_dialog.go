package ui

import (
	"context"
	"strings"
	"time"

	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"

	"kafak-tools/internal/adapter"
	"kafak-tools/internal/config"
)

var securityModes = []string{"plaintext", "ssl"}

// openConnDialog 连接配置对话框：类型自选（Kafka/ActiveMQ），表单随类型切换。
// existing 为 nil 时新建。返回是否已保存。
func openConnDialog(owner walk.Form, store *config.Store, existing *config.Connection) (bool, error) {
	isNew := existing == nil
	conn := config.Connection{
		ID:       config.NewID(),
		Type:     config.TypeKafka,
		Kafka:    config.DefaultKafka(),
		ActiveMQ: config.DefaultActiveMQ(),
	}
	title := "新建连接"
	if existing != nil {
		conn = *existing
		title = "编辑连接 - " + existing.Name
	}

	var (
		dlg                                  *walk.Dialog
		nameEdit, bootEdit, trustEdit        *walk.LineEdit
		keyEdit, sslPassEdit, groupEdit      *walk.LineEdit
		jolURLEdit, jolUserEdit, jolPassEdit *walk.LineEdit
		stompEdit                            *walk.LineEdit
		typeCB, secCB                        *walk.ComboBox
		stompChk                             *walk.CheckBox
		kafkaBox, amqBox                     *walk.GroupBox
		saveBtn, cancelBtn, testBtn          *walk.PushButton
	)

	typeIdx, secIdx := 0, 0
	if conn.Type != config.TypeKafka {
		typeIdx = 1
	}
	if conn.Kafka.Security == "ssl" {
		secIdx = 1
	}

	gather := func() config.Connection {
		c := conn // 保留 ID
		c.Name = strings.TrimSpace(nameEdit.Text())
		if typeCB.CurrentIndex() == 0 {
			c.Type = config.TypeKafka
		} else {
			c.Type = config.TypeActiveMQ
		}
		sec := "plaintext"
		if secCB.CurrentIndex() == 1 {
			sec = "ssl"
		}
		c.Kafka = config.Kafka{
			Bootstrap:   strings.TrimSpace(bootEdit.Text()),
			Security:    sec,
			Truststore:  strings.TrimSpace(trustEdit.Text()),
			Keystore:    strings.TrimSpace(keyEdit.Text()),
			SSLPassword: sslPassEdit.Text(),
			GroupID:     strings.TrimSpace(groupEdit.Text()),
		}
		c.ActiveMQ = config.ActiveMQ{
			JolokiaURL:   strings.TrimSpace(jolURLEdit.Text()),
			User:         jolUserEdit.Text(),
			Password:     jolPassEdit.Text(),
			STOMPAddr:    strings.TrimSpace(stompEdit.Text()),
			STOMPEnabled: stompChk.Checked(),
		}
		return c
	}

	saved := false

	// JKS 文件选择（truststore/keystore 都是文件，避免手打路径出错）
	browseJKS := func(target *walk.LineEdit, title string) {
		if target == nil {
			return
		}
		fd := &walk.FileDialog{
			Title:  title,
			Filter: "Java KeyStore (*.jks)|*.jks|All Files (*.*)|*.*",
		}
		if p := strings.TrimSpace(target.Text()); p != "" {
			fd.FilePath = p
		}
		accepted, err := fd.ShowOpen(dlg)
		if err != nil {
			walk.MsgBox(dlg, "打开文件失败", err.Error(), walk.MsgBoxIconError)
			return
		}
		// ShowOpen（单选）结果在 FilePath；FilePaths 仅 ShowOpenMultiple 填充
		if accepted && fd.FilePath != "" {
			target.SetText(fd.FilePath)
		}
	}

	// 安全选项联动端口：SSL→38131，明文→38132（仅当当前值是默认/双口才改，自定义不动）
	syncBootstrapBySecurity := func() {
		if bootEdit == nil || secCB == nil {
			return
		}
		const (
			sslDef   = "127.0.0.1:38131"
			plainDef = "127.0.0.1:38132"
			dualDef  = "127.0.0.1:38131,127.0.0.1:38132"
		)
		cur := strings.TrimSpace(bootEdit.Text())
		if secCB.CurrentIndex() == 1 { // ssl
			if cur == "" || cur == plainDef || cur == dualDef {
				bootEdit.SetText(sslDef)
			}
		} else { // plaintext
			if cur == "" || cur == sslDef || cur == dualDef {
				bootEdit.SetText(plainDef)
			}
		}
	}

	syncTypeVisibility := func() {
		if kafkaBox == nil || amqBox == nil {
			return
		}
		isK := typeCB != nil && typeCB.CurrentIndex() == 0
		kafkaBox.SetVisible(isK)
		amqBox.SetVisible(!isK)
	}

	d := Dialog{
		AssignTo:     &dlg,
		Title:        title,
		Size:         Size{Width: 600, Height: 560},
		FixedSize:    false,
		Layout:       VBox{Spacing: 8},
		CancelButton: &cancelBtn,
		Children: []Widget{
			Composite{Layout: Grid{Columns: 2, Spacing: 6}, Children: []Widget{
				Label{Text: "名称:"},
				LineEdit{AssignTo: &nameEdit, Text: conn.Name, CueBanner: "例如: 本机 Kafka"},
				Label{Text: "类型:"},
				ComboBox{
					AssignTo:              &typeCB,
					Model:                 []string{"Kafka", "ActiveMQ (JMS)"},
					CurrentIndex:          typeIdx,
					OnCurrentIndexChanged: func() { syncTypeVisibility() },
				},
			}},
			GroupBox{
				AssignTo: &kafkaBox,
				Title:    "Kafka 连接参数",
				Layout:   Grid{Columns: 2, Spacing: 6},
				Children: []Widget{
					Label{Text: "Bootstrap:"},
					LineEdit{AssignTo: &bootEdit, Text: conn.Kafka.Bootstrap, CueBanner: "明文:127.0.0.1:38132 ｜ SSL:127.0.0.1:38131（随安全选项切换）"},
					Label{Text: "安全协议:"},
					ComboBox{
						AssignTo:     &secCB,
						Model:        securityModes,
						CurrentIndex: secIdx,
						OnCurrentIndexChanged: func() {
							syncBootstrapBySecurity()
						},
					},
					Label{Text: "Truststore:"},
					Composite{Layout: HBox{Spacing: 4}, Children: []Widget{
						LineEdit{AssignTo: &trustEdit, Text: conn.Kafka.Truststore, CueBanner: "ssl 模式必填，点右侧…选择 .jks 文件", StretchFactor: 1},
						PushButton{
							Text:        "…",
							MinSize:     Size{Width: 40, Height: 0},
							ToolTipText: "选择 truststore 文件",
							OnClicked:   func() { browseJKS(trustEdit, "选择 Truststore（JKS 文件）") },
						},
					}},
					Label{Text: "Keystore:"},
					Composite{Layout: HBox{Spacing: 4}, Children: []Widget{
						LineEdit{AssignTo: &keyEdit, Text: conn.Kafka.Keystore, CueBanner: "ssl 双向认证时填写，可空", StretchFactor: 1},
						PushButton{
							Text:        "…",
							MinSize:     Size{Width: 40, Height: 0},
							ToolTipText: "选择 keystore 文件",
							OnClicked:   func() { browseJKS(keyEdit, "选择 Keystore（JKS 文件）") },
						},
					}},
					Label{Text: "SSL 密码:"},
					LineEdit{AssignTo: &sslPassEdit, Text: conn.Kafka.SSLPassword, PasswordMode: true},
					Label{Text: "消费组:"},
					LineEdit{AssignTo: &groupEdit, Text: conn.Kafka.GroupID, CueBanner: "opt-consumer"},
				},
			},
			GroupBox{
				AssignTo: &amqBox,
				Title:    "ActiveMQ (JMS) 连接参数",
				Layout:   Grid{Columns: 2, Spacing: 6},
				Children: []Widget{
					Label{Text: "Jolokia URL:"},
					LineEdit{AssignTo: &jolURLEdit, Text: conn.ActiveMQ.JolokiaURL, CueBanner: "http://127.0.0.1:8161/api/jolokia"},
					Label{Text: "用户名:"},
					LineEdit{AssignTo: &jolUserEdit, Text: conn.ActiveMQ.User},
					Label{Text: "密码:"},
					LineEdit{AssignTo: &jolPassEdit, Text: conn.ActiveMQ.Password, PasswordMode: true},
					Label{Text: "STOMP 地址:"},
					LineEdit{AssignTo: &stompEdit, Text: conn.ActiveMQ.STOMPAddr, CueBanner: "127.0.0.1:61613（服务端需启用 stomp 连接器）"},
					Label{Text: "实时订阅:"},
					CheckBox{
						AssignTo: &stompChk,
						Text:     "启用 STOMP（实时 topic 订阅）",
						Checked:  conn.ActiveMQ.STOMPEnabled,
					},
				},
			},
			Composite{Layout: HBox{Spacing: 6}, Children: []Widget{
				PushButton{
					AssignTo: &testBtn,
					Text:     "测试连接",
					OnClicked: func() {
						c := gather()
						if err := c.Validate(); err != nil {
							walk.MsgBox(dlg, "格式检查未通过", err.Error(), walk.MsgBoxIconWarning)
							return
						}
						// 真实连通测试（异步，避免卡界面）
						testBtn.SetEnabled(false)
						go func() {
							var msg string
							var icon walk.MsgBoxStyle
							ad, err := adapter.New(c)
							if err != nil {
								msg, icon = err.Error(), walk.MsgBoxIconError
							} else {
								ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
								err = ad.Test(ctx)
								cancel()
								if err != nil {
									msg, icon = "连接失败：\n"+err.Error(), walk.MsgBoxIconError
								} else {
									msg, icon = "连接成功！", walk.MsgBoxIconInformation
								}
							}
							dlg.Synchronize(func() {
								testBtn.SetEnabled(true)
								walk.MsgBox(dlg, "测试连接", msg, icon)
							})
						}()
					},
				},
				HSpacer{StretchFactor: 1},
				PushButton{
					AssignTo: &saveBtn,
					Text:     "保存",
					OnClicked: func() {
						c := gather()
						if err := c.Validate(); err != nil {
							walk.MsgBox(dlg, "校验失败", err.Error(), walk.MsgBoxIconWarning)
							return
						}
						store.Upsert(c)
						if err := store.Save(); err != nil {
							// 回滚内存态，保持与磁盘一致
							if isNew {
								store.Remove(c.ID)
							} else {
								store.Upsert(*existing)
							}
							walk.MsgBox(dlg, "保存失败", err.Error(), walk.MsgBoxIconError)
							return
						}
						saved = true
						dlg.Accept()
					},
				},
				PushButton{
					AssignTo:  &cancelBtn,
					Text:      "取消",
					OnClicked: func() { dlg.Cancel() },
				},
			}},
		},
	}

	if err := d.Create(owner); err != nil {
		return false, err
	}
	// 按初始类型显示对应表单
	kafkaBox.SetVisible(typeIdx == 0)
	amqBox.SetVisible(typeIdx != 0)

	dlg.Run()
	return saved, nil
}
