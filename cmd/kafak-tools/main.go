package main

import (
	"flag"
	"log"

	"kafak-tools/internal/ui"
)

func main() {
	// -smoke: 建窗→1.5s 后自动关闭，用于无头/CI 环境冒烟
	smoke := flag.Bool("smoke", false, "create main window and exit automatically (smoke test)")
	// -uitest: 自动点按钮序列（选连接/查询/翻页/发送），崩溃会落盘 crash.log
	uitest := flag.Bool("uitest", false, "run automated UI click sequence for crash hunting")
	flag.Parse()

	log.SetFlags(log.LstdFlags)
	if err := ui.Run(*smoke, *uitest); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
