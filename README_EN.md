<div align="center">
  <img src="https://user-images.githubusercontent.com/10267910/123536153-28b6da80-d75b-11eb-90ff-4b9e5a7d7d8c.png">
  <h1>Dwarf</h1>
  An open source solution for automatic data labeling and management.
</div>

## Features

- Automatically discover new files.
- Tagging the image and video file automatically and manually.
- All tags saved in database.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

RabbitMQ: Messaging.

MongoDB: Tag storage.

Others: Install with requirements.txt

```bash
python3 -m pip install -r requirement.txt
```

### Installing

Get the source code.

```bash
# From your favorite development directory
git clone https://github.com/yinguobing/dwarf.git
```

## Setup
You need to setup a few parameters in the config file `config.yml`.

### Setup the directories

There are two directories to be set. `barn` is the directory that will be monitored. All new files will be processed automatically. `warehouse` is the directory in which the new file will be saved.

```yaml
dirs:
  barn: /path/to/barn
  warehouse: /path/to/warehouse 
```

### Setup the file types to be monitored
All file types of interest is defined by the filename extension.

```yaml
video_types: ["avi", "mp4"]
image_types: ["jpg", "jpeg", "png", "gif", "bmp"]
```

### Setup the RabbitMQ

You can install RabbitMQ by following the official instructions. Or, you can run a quick instance with docker. Please note the default port `5672` will be used.

```bash
sudo docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Then, setup the rabbit in the config file:

```yaml
rabbitmq:
  address: "localhost"
  queue: "file_list"
```

### Setup the database

First you need to setup a databse manually for dwarf to use. Here is an example:

- the database could be reached by host `localhost` and port `27017`.
- the database is authorized by username `mongoadmin` and password `secret`.
- the database name is `dwarf`

You can set the collection name for videos and images here.

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

## Running

Make sure the current use has the permission of writing files in `barn` and `warehouse`, then run:

```bash
python3 main.py
```

You can find the log in the log file `dwarf.log`.

## Authors
Yin Guobing (?????????) - [yinguobing](https://yinguobing.com)

## License
![GitHub](https://img.shields.io/github/license/yinguobing/dwarf)
