#!/usr/bin/env bash
#Usage:
#       调用稽核规则 计算入库
#fileName:RuleScheduling.sh

#脚本存放位置及中间临时文件位置
tabdir=/home/test

#目标表 - 清单数据
targetListCol(){
	#清空目标数据
	hive -e "use $1;truncate table bdat_chk_v4_list_gis"
	hive -e "use $1;truncate table bdat_chk_v4_list_spc"
	hive -e "use $1;truncate table bdat_chk_v4_list_trs"
	hive -e "use $1;truncate table bdat_chk_v4_list_wls"
    #获取目标表需要的字段信息
	#表结构导出 --清单表表结构一致所有选取一个即可
	hive -e "use $1;desc bdat_chk_v4_list_spc" > ${tabdir}/tmp/bdat_chk_v4_list_spc.lst
	#目标列
	col=`cat ${tabdir}/tmp/bdat_chk_v4_list_spc.lst | grep -iE "string|double|int" | awk '{print $1}' | tr '\n' ' '`
	result=`echo ${col% *}`
	#由于存在冗余字段 故需要特殊处理
	redundance="region_name,district_name"
	echo "[ targetListCol ] start ..."
	checkColAndInsert "$1" "bdat_chk_v4_rules" "${result}" "chk_sql" "${redundance}" "list"
	echo "[ targetListCol ] finish !"
}
#目标表 - 统计数据
targetResCol(){
	#清空目标数据
	hive -e "use $1;truncate table bdat_chk_v4_res_count"
    #获取目标表需要的字段信息
	#表结构导出
	hive -e "use $1;desc bdat_chk_v4_res_count" > ${tabdir}/tmp/bdat_chk_v4_res_count.lst
	#目标列
	col=`cat ${tabdir}/tmp/bdat_chk_v4_res_count.lst | grep -iE "string|double|int" | awk '{print $1}' | tr '\n' ' '`
	result=`echo ${col% *}`
	#由于存在冗余字段 故需要特殊处理
	redundance="region_name"
	echo "[ targetResCol ] start ..."
	checkColAndInsert "$1" "bdat_chk_v4_res_rules" "${result}" "v_sql" "${redundance}" "count"
	echo "[ targetResCol ] finish !"
}

#规则表解析and数据导入
checkColAndInsert(){
    #判断是哪个规则：稽核规则和统源统计规则
	#表数据导出
    hive -e "use $1;insert overwrite local directory \"${tabdir}/tmp/$2\" row format delimited fields terminated by '\001' select * from $2 limit 10;"
	#表结构导出
	hive -e "use $1;desc $2" > ${tabdir}/tmp/$2.lst
	flag=`ls -l ${tabdir}/tmp/$2 | grep -v "total" | wc -l`
	#判断导出文件数
    if [ $flag -ge 1 ]
    then
        #echo "稽核规则表导出文件数大于等于1个"
        range=(`ls -l ${tabdir}/tmp/$2 | grep -v "total" | awk '{print $9}'`)
		len=${#range[@]}
		#初始化文件
		if [ -f ${tabdir}/tmp/$2.txt ]
		then
			rm -rf ${tabdir}/tmp/$2.txt
		fi
		#合成单一文件 -文件大小被截为0字节
		for((i=0;i<$len;i++))
		do
			cat ${tabdir}/tmp/$2/${range[$i]} >>${tabdir}/tmp/$2.txt
		done
    fi
	#解析入库
	#由于导出数据的行分隔符为\n，则表中字段具体指不可能存在\n --不然将导致数据错乱造成结果不正常
	array_b=($3)
	array_c=(`cat ${tabdir}/tmp/$2.lst | grep -iE "string|double|int" | awk '{print $1}'`)
	len_col=${#array_c[@]}
	for((j=0;j<$len_col;j++))
	do
		#稽核SQL获取
		if [ "$4" == "${array_c[$j]}" ]
		then
			n=`expr $j + 1`
			sql=`cat ${tabdir}/tmp/$2.txt | awk -F '\001' '{print $'$n'"\001"}'`
		fi
		#专业获取
		if [ "${array_c[$j]}" == "speiciality" ]
		then
			n=`expr $j + 1`
			speiciality=`cat ${tabdir}/tmp/$2.txt | awk -F '\001' '{print $'$n'"\001"}'`
		fi
	done
	#字段赋值
	#定义新的数组对应目标表字段
	#初始化m
	m="\""
	for ((x=0;x<${#array_b[@]};x++))
	do
		cat ${tabdir}/tmp/$2.lst | grep -iE "string|double|int"  | awk '{print $1}' | grep "${array_b[$x]}" > /dev/null 2>&1
		if [ $? -eq 0 ]
		then
			echo "[ checkColAndInsert ] ${array_b[$x]} exists $2"
			for((j=0;j<$len_col;j++))
			do
				if [ "${array_b[$x]}" == "${array_c[$j]}" ]
				then
					echo "[ checkColAndInsert ] match columns"
					n=`expr $j + 1`
					m=$m"\\\"\"\$$n\"\\\","
				fi
			done
		else
			#存在则说明是目标表需要数据
			#需要主要此处只能匹配
			echo $5 | grep -i "${array_b[$x]}" > /dev/null 2>&1
			#判断子查询涉及的字段，不存在的则说明是冗余字段，给予NULL即可
			if [ $? -eq 0 ]
			then
				m=$m"NULL,"
			else
				m=$m"${array_b[$x]},"
			fi
		fi
	done
	#m末尾处理
	mm=`echo ${m%,*}`
	m=$mm"\001\""
	#多线程
	#创建管道
	mkfifo testfifo
	exec 6<>testfifo
	rm -rf testfifo
	#并发数控制
	thread=4
	for ((i=0;i<=$thread;i++))
	do
	 echo >&6
	done
	#字段详情
	columns=`cat ${tabdir}/tmp/$2.txt | awk -F '\001' '{print '$m'}'`
	#数据导入
	#更改默认分隔符
	OLD_IFS=$IFS
	IFS=$'\\\001'
	array_c=($columns)
	echo "sql-----"$sql
	array_sql=($sql)
	array_d=($speiciality)
	IFS=$OLD_IFS
	if [ ${#array_c[@]} -eq ${#array_sql[@]} ]
	then
		echo "[ checkColAndInsert ] chk_sql is normal"
		for((y=0;y<${#array_c[@]};y++))
		do
		read -u6
		{
			#字段命名方式存在差异需要分开处理
			if [ $6 == "list" ]
			then

				var="'"
				columnss=`echo ${array_c[$y]}| sed 's/"/'$var'/g'`
				#剔除空格操作
				spe=`echo ${array_d[$y]} | sed s/[[:space:]]//g`
				case ${spe} in
				"传输外线")
				echo "[ checkColAndInsert ] 传输外线专业"
				echo "[ checkColAndInsert ] hive -e \"use $1;insert into table bdat_chk_v4_list_gis select ${columnss} from (${array_sql[$y]}) t where iscorrect=1\""
				hive -e "use $1;insert into table bdat_chk_v4_list_gis select ${columnss} from (${array_sql[$y]}) t where iscorrect=1"
				;;
				"空间")
				echo "[ checkColAndInsert ] 空间专业"
				echo "[ checkColAndInsert ] hive -e \"use $1;insert into table bdat_chk_v4_list_spc select ${columnss} from (${array_sql[$y]}) t where iscorrect=1\""
				hive -e "use $1;insert into table bdat_chk_v4_list_spc select ${columnss} from (${array_sql[$y]}) t where iscorrect=1"
				;;
				"传输内线")
				echo "[ checkColAndInsert ] 传输内线专业"
				echo "[ checkColAndInsert ] hive -e \"use $1;insert into table bdat_chk_v4_list_trs select ${columnss} from (${array_sql[$y]}) t where iscorrect=1\""
				hive -e "use $1;insert into table bdat_chk_v4_list_trs select ${columnss} from (${array_sql[$y]}) t where iscorrect=1"
				;;
				"无线")
				echo "[ checkColAndInsert ] 无线专业"
				echo "[ checkColAndInsert ] hive -e \"use $1;insert into table bdat_chk_v4_list_wls select ${columnss} from (${array_sql[$y]}) t where iscorrect=1\""
				hive -e "use $1;insert into table bdat_chk_v4_list_wls select ${columnss} from (${array_sql[$y]}) t where iscorrect=1"
				;;
				*)
				echo "[ checkColAndInsert ] speiciality not in (传输外线\空间\传输内线\无线)"
				esac
			elif [ $6 == "count" ]
			then
				#剔除匹配多余的字段，模型存在问题，没有规律，只能写死
				var="'"
				a=`echo ${array_c[$y]} | awk -F 'res_count' '{print $1" res_count, "}'`
				b=`echo ${array_c[$y]} | awk -F ',' '{print $NF}'`
				columnss=`echo $a$b`
				echo "[ checkColAndInsert ] 质量资源统计"
				echo "[ checkColAndInsert ] hive -e \"use $1;insert into table bdat_chk_v4_res_count select ${columnss} from (${array_sql[$y]}) t\""
				hive -e "use $1;insert into table bdat_chk_v4_res_count select ${columnss} from (${array_sql[$y]}) t"
			else
				echo "[ checkColAndInsert ] paramter is abnormal,please check and try again"
				exit 0
			fi

			echo >&6
		}&
		done
		wait
		exec 6>&-
		exec 6<&-
	else
		echo "[ checkColAndInsert ] chk_sql is abnormal,please check and try again"
		exit 0
	fi
}

#程序入口
#---------------------参数
if [ $# -ge 1 ]
 then
  echo "Usage: 调用稽核规则 计算入库
   sh RuleScheduling xjmov_health_db
   xjmov_health_db:库信息"
  schema=$1
  #稽核清单
  #targetListCol $schema
  #质量资源统计
  targetResCol $schema
else
  echo "入参错误,请参考
   Usage:
   sh RuleScheduling xjmov_health_db
   xjmov_health_db:库信息"
   exit 0
fi
