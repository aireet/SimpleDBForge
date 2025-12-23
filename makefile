.PHONY: proto install-tools clean-proto check-tools

# 安装必要的工具
install-tools:
	@echo "安装 protobuf 工具..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0
	@echo "工具安装完成"

# 生成 protobuf 代码
proto: install-tools
	@echo "生成 protobuf 代码..."
	protoc --go_out=. \
	--go_opt=paths=source_relative \
	api/sdbf/*.proto
	@echo "protobuf 代码生成完成"
