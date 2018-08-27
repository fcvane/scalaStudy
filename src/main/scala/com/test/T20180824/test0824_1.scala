package com.test.T20180824

import com.alibaba.fastjson.JSON

class test0824_1 {

}

object test0824_1 extends App {
  /**
    * 解析json
    */

  val str2 = "{\n    \"$schema\":\"http://json-schema.org/draft-04/schema#\",\n    \"title\":\"OGG.STEST1\",\n    \"description\":\"JSON schema for table OGG.STEST1\",\n    \"definitions\":{\n        \"row\":{\n            \"type\":\"object\",\n            \"properties\":{\n                \"ID\":{\n                    \"type\":[\n                        \"number\",\n                        \"null\"\n                    ]\n                },\n                \"NAME\":{\n                    \"type\":[\n                        \"string\",\n                        \"null\"\n                    ]\n                }\n            },\n            \"additionalProperties\":false\n        },\n        \"tokens\":{\n            \"type\":\"object\",\n            \"description\":\"Token keys and values are free form key value pairs.\",\n            \"properties\":{\n            },\n            \"additionalProperties\":true\n        }\n    },\n    \"type\":\"object\",\n    \"properties\":{\n        \"table\":{\n            \"description\":\"The fully qualified table name\",\n            \"type\":\"string\"\n        },\n        \"op_type\":{\n            \"description\":\"The operation type\",\n            \"type\":\"string\"\n        },\n        \"op_ts\":{\n            \"description\":\"The operation timestamp\",\n            \"type\":\"string\"\n        },\n        \"current_ts\":{\n            \"description\":\"The current processing timestamp\",\n            \"type\":\"string\"\n        },\n        \"pos\":{\n            \"description\":\"The position of the operation in the data source\",\n            \"type\":\"string\"\n        },\n        \"primary_keys\":{\n            \"description\":\"Array of the primary key column names.\",\n            \"type\":\"array\",\n            \"items\":{\n                \"type\":\"string\"\n            },\n            \"minItems\":0,\n            \"uniqueItems\":true\n        },\n        \"tokens\":{\n            \"$ref\":\"#/definitions/tokens\"\n        },\n        \"before\":{\n            \"$ref\":\"#/definitions/row\"\n        },\n        \"after\":{\n            \"$ref\":\"#/definitions/row\"\n        }\n    },\n    \"required\":[\n        \"table\",\n        \"op_type\",\n        \"op_ts\",\n        \"current_ts\",\n        \"pos\"\n    ],\n    \"additionalProperties\":false\n}"
  val json = JSON.parseObject(str2)
  //获取成员
  val fet = json.get("et")
  //返回字符串成员
  val etString = json.getString("table")
  println(etString)
  //返回整形成员
  val vtm = json.getInteger("vtm")
  println(vtm)
  //返回多级成员
  //  val client = json.getJSONObject("body").get("client")
  //  println(client)
  val str = "{\"table\":\"OGG.STEST1\",\"op_type\":\"U\",\"op_ts\":\"2018-08-25 07:33:54.343488\",\"current_ts\":\"2018-08-25T15:34:00.029000\",\"pos\":\"00000000000000003354\",\"before\":{\"ID\":1,\"NAME\":\"A\"},\"after\":{\"ID\":1,\"NAME\":\"A\"}}"
  val json1 = JSON.parseObject(str)
  //返回字符串成员
  val tab = json1.getString("table")
  println(tab)
  val data = json1.getJSONObject("after").get("NAME")
  println(data + "-------------------")

}