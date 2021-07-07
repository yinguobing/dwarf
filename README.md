<div align="center">
  <img src="https://user-images.githubusercontent.com/10267910/123536153-28b6da80-d75b-11eb-90ff-4b9e5a7d7d8c.png">
  <h1>Dwarf</h1>
  视频与图像数据的自动化整理与标记。
</div>

## 功能

- 自动发现新文件。
- 支持自动与手动标签。
- 文件集中存储，标签数据库存储。

## 快速开始

以下内容介绍了如何在本机部署并启用Dwarf服务。

### 依赖安装

Dwarf依赖以下应用与服务包。在运行之前需要安装完成。

#### RabbitMQ

版本 3.8.18。用于消息队列。可遵循官网安装教程。或者使用Docker服务。

官方安装指南：https://www.rabbitmq.com/download.html。

Docker参考命令：

```bash
sudo docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

#### MongoDB

版本 4.4。用于标签存储。

官方安装教程：https://docs.mongodb.com/manual/installation/

#### FFMPEG

用于视频标签提取。可以使用系统包管理器安装，也可以自行编译安装。包管理器安装方式：

```bash
# Ubuntu
sudo apt install ffmpeg
```

#### 其它Python相关依赖

可以使用 pip requirements.txt 安装。

```bash
python3 -m pip install -r requirement.txt
```

### 应用安装

当前直接使用源码。在目标机器上使用git clone代码。

```bash
git clone https://github.com/yinguobing/dwarf.git
```

## 服务设定

Dwarf使用配置文件 `config.yml` 来设定服务行为。可以使用任意文本编辑器编辑该配置文件。服务启动时会自动加载该配置文件。

### 设置文件目录

Dwarf服务当前会使用两个目录。

第一个目录是 `barn`，待处理的文件需要放置在此文件夹中。
第二个目录是 `warehouse` ，处理后的文件会存储在此文件夹中。

```yaml
dirs:
  barn: /path/to/barn
  warehouse: /path/to/warehouse 
```

### 设置待处理文件类型

Dwarf使用文件后缀名来判定文件类型。将文件后缀名以文本的形式添加到配置文件即可。不区分大小写。

```yaml
video_types: ["avi", "mp4"]
image_types: ["jpg", "jpeg", "png", "gif", "bmp"]
```

### 设置消息队列

当前使用RabbitMQ。在配置文件中指定消息服务地址与队列名称。

```yaml
rabbitmq:
  address: "localhost"
  queue: "file_list"
```

### 设置数据库

在启用Dwarf服务前，需要为其创建专用的数据库。例如：

- 数据库访问地址 `localhost` 端口 `27017`。
- 数据库名称为 `dwarf`
- 数据库通过用户名 `mongoadmin` 与密码 `secret` 授权访问，该用户具备读写权限。

将以上信息填入配置文件如下：

```yaml
mongodb:
  host: "localhost"
  port: 27017
  username: "mongoadmin"
  password: "secret"
  name: "dwarf"
  collections:
    images: "images"
    videos: "videos"
```

其中 `images` 与 `videos` 为collection的名称。Dwarf会自动创建。

## 权限配置

Dwarf服务需要具备目标文件夹的读写权限。假设用于该服务运行的用户名为`dwarf`，可为其更改目录权限。以`barn`目录为例：

```bash
sudo chown dwarf:dwarf barn
```

## 运行

首先切换到该服务的执行用户，例如：

```bash
su dwarf
```

之后执行以下命令开启服务：

```bash
python3 main.py
```

示例输出如下：

```bash
$ python3 main.py
2021-07-07 10:36:33,121 root     INFO     Porter is ready.
2021-07-07 10:36:33,125 root     INFO     Stocker is ready.
2021-07-07 10:36:33,138 root     INFO     Clark is ready.
2021-07-07 10:36:33,138 root     INFO     Steward is ready.
2021-07-07 10:36:33,138 porter   INFO     [*] Monitoring...
2021-07-07 10:36:33,144 steward  INFO     [*] Waiting for messages...
```

如果需要使用其它配置文件，可以在命令中指定：

```bash
python3 main.py myconfig.yml
```

处理日志在文件 `dwarf.log` 中。

## 使用方法

录入Dwarf系统的文件需要满足以下条件：

- 是当前系统所支持的文件格式。例如视频，图像。
- 需要为文件指定标签与作者。

将满足条件的文件整理后放置到监控目录下即可，系统会自动将文件分析归档。

例如现有来自园区入口监控的视文件若干，需要录入系统，可按如下步骤实现。

1. 在监控目录 `barn` 下新建作业目录，名称自行指定，例如 `upload`。
2. 在作业目录下新建 `tags.txt` 文本文档，在其中手动指定文件标签，使用**空格**分隔，不要换行。例如 `花园 草坪 猫 狗`。手动标签的意义在于方便后期回溯查找。
3. 在作业目录下新建 `authors.txt` 文本文档，在其中手动录入作者。多个作者可使用**空格**分隔，不要换行。例如 `李白 杜甫 李清照`

此时目录树的状况如下：

```bash
barn
└── upload
    ├── authors.txt
    └── tags.txt
```

最后一步，将视频文件拖入该目录下即可。

## 注意事项

以下表现为服务的预期行为

- 处理完成后的文件会被自动归档并从作业目录消失。
- 无法处理的文件会留在原地。
- 如果存在多级目录，默认使用根目录标签与作者。
- 如果存在多级目录，且当前目录与根目录存在标签与作者，则当前目录优先。

以下文件可能无法被处理

- 不支持的文件格式。
- 不完整的文件，例如解码失败的视频文件，损坏的图像文件。
- 没有读取权限的文件。
- 没有配套标签与作者的文件。
- 在写入数据库时失败的文件。

## 异常处理

### 处理失败原因查询

如果文件无法被处理，可以通过日志文件查询具体原因。

### 手动执行

如果目录下存在之前无法被处理的文件（例如`tags.txt`文件缺失），但是经过处理后满足处理条件的情况下（例如补上了缺失的`tags.txt`文件），可以在当前目录下新建`dwarf.run`文件手动触发处理流程。
