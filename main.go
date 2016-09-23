package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"common/goracle"
	"common/goracle/connect"
	"common/ini"
	//	"common/utils"
	"github.com/garyburd/redigo/redis"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)

func main() {

	conf := "load.conf"

	if len(os.Args) > 1 {
		conf = os.Args[1]
	}

	finfo, err := os.Stat(conf)
	if err != nil {
		// no such file or dir
		logger.Printf("file %s is not exists", conf)
		return
	}
	if !finfo.IsDir() {
		// it's a file
	} else {
		// it's a directory
		logger.Println("you specified a directory")
		return
	}

	cfg := ini.DumpAll(conf)

	uid := cfg["DEFAULT:"+"uid"]
	url := cfg["DEFAULT:"+"url"]
	passwd := cfg["DEFAULT:"+"passwd"]
	sql := cfg["options:"+"sql"]
	col := cfg["options:"+"col"]
	batch, err := strconv.Atoi(cfg["DEFAULT:"+"batch"])
	if err != nil {
		logger.Println("无效的批量大小")
		batch = 10000
	}

	logger.Println("=========parameters==========")
	logger.Printf("%-12s: %s\n", "uid", uid)
	logger.Printf("%-12s: %s\n", "password", passwd)
	logger.Printf("%-12s: %s\n", "sql text", sql)
	logger.Printf("%-12s: %s\n", "field list", col)
	logger.Printf("%-12s: %d\n", "batch size", batch)

	// logger.Println("redis url :" + url)
	// logger.Println("password  :" + passwd)
	// logger.Println("sql   text:" + sql)
	// logger.Println("field list:" + col)
	// logger.Println("batch size:" + batch)
	logger.Println("=============================")

	err = loadTask(uid, sql, col, url, passwd, batch)
	if err != nil {
		logger.Println("加载出错:", err)
	}

}

func loadTask(uid, sql, cols, url, passwd string, batch int) error {

	conn, err := connect.GetRawConnection(uid)
	if err != nil {
		logger.Printf("连接数据库发生错误.\n连接信息为: %s\n错误信息为: %s", uid, strings.Split(err.Error(), "\n")[1])
		return err
	}
	defer conn.Close()

	cur := conn.NewCursor()
	defer cur.Close()

	err = cur.Execute(sql, nil, nil)

	// logger.Println(sql)
	if err != nil {
		logger.Printf("执行sql 语句发生错误.\nsql 语句为: %s\n错误信息为: %s", sql, strings.Split(err.Error(), "\n")[1])
		return err
	}

	// 获取sql 的列别名
	columns, err := goracle.GetColumns(cur)
	if err != nil {
		logger.Printf("获取列信息发生错误.\n错误信息为: %s", strings.Split(err.Error(), "\n")[1])
		return err
	}

	// records 为全部记录 records[i][j]=v
	records, err := cur.FetchMany(batch) //[][]interface{}

	for err == nil && len(records) > 0 {

		err = loadData(records, url, passwd, strings.Split(cols, ","), columns)

		records, err = cur.FetchMany(batch)
	}
	if err != nil {
		logger.Printf("获取结果集失败.\n错误信息为: %s", strings.Split(err.Error(), "\n")[1])
		return err
	}

	return nil
}

func loadData(records [][]interface{}, url string, passwd string, fields []string, columns []goracle.Column) error {

	N := len(columns)

	rs, err := redis.Dial("tcp", url)
	if err != nil {
		logger.Printf("连接 redis 发生错误", err, url)
		return err
	}

	if passwd != "" {
		if _, err := rs.Do("AUTH", passwd); err != nil {
			logger.Println("AUTH fail.")
			rs.Close()
			return err
		}
	}

	for _, row := range records { //每行
		argx := make([]interface{}, N*2, N*2)
		idx := 0
		for i, col := range row { //每列
			r := ""
			if col == nil {
				r = ""
			} else {
				r = strings.TrimSuffix(strings.TrimPrefix(columns[i].String(col), `"`), `"`)
			}
			argx[idx] = fields[i] //fieldnames
			idx++
			argx[idx] = r //field value
			idx++

		}
		//    1                            2n-2   2n-1
		//key xx field1 xx1 field2 xx2 ... fieldn nnn
		rs.Send("HMSET", argx[1:N*2]...)
	}

	err = rs.Flush()
	if err != nil {
		logger.Printf("ERROR flush 出错\n", err)
		return err
	} else {

		logger.Println("加载成功,本次加载:", len(records))
	}

	for i := 0; i < len(records); i++ {
		_, err = rs.Receive()
		if err != nil {
			logger.Printf("ERROR 获取reply 失败:\n", err)
			break
		}
	}

	rs.Close()
	return err

}
