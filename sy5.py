import pandas as pd 
from glob import glob
import os
import time
import pymysql


pd.set_option('display.float_format',lambda x : '%.f' % x)#禁用科学计数法显示

csvs = glob('E:\python\物联网云端数据处理实验-5-2018119161-廖瑞金\\*.csv')

pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)



#链接数据库，并将csv文件导入数据库中
#链接数据库

try:
     conn = pymysql.connect(host='localhost', user='root', password='Root', db='test', port=3306, charset='utf8')
     cur = conn.cursor()
     print('数据库连接成功！')
     print(' ')
     
except:
     print('数据库连接失败！')
  

      
   
#导入文件load_csv函数，参数分别为csv文件路径，表名称，数据库名称
def load_csv(conn,cur):
    #读取任意文件夹下的csv文件
    #获取程序所在路径及该路径下所有文件名称
    path = os.getcwd()
    files = os.listdir(path)
     #遍历所有文件 
    i = 0
    for file in files:
         #判断文件是不是csv文件
        if file.split('.')[-1] in ['csv']: 
             i += 1
             #构建一个表名称，供后期SQL语句调用
             filename = file.split('.')[0]
             filename = 'tab_' + filename 
             print(filename)
             #使用pandas库读取csv文件的所有内容,结果f是一个数据框，保留了表格的数据存储方式，是pandas的数据存储结构。
             f = pd.read_csv(file, encoding='gbk') # 注意：如果报错就改成 encoding='utf-8' 试试
             #print(f)
             #columns = f.columns.tolist() 
             #print(columns)
              
             # 将csv文件中的字段类型转换成mysql中的字段类型
             types =list(map(str, f.dtypes))
             #print(types)
             field = f.columns.tolist() #用来接收字段名称的列表
             table = [] #用来接收字段名称和字段类型的列表 
             for item in range(0,len(types)):
                  if 'int' in types[item]:
                      char = ' INT'
                  elif 'float' in types[item]:
                      char =' FLOAT'
                  elif 'object' in types[item]:
                      char = ' VARCHAR(255)'
                  elif 'datetime' in types[item]:
                      char =' DATETIME'
                  else:
                      char = item + ' VARCHAR(255)'
                  table.append(field[item] + char)
           
             #  构建SQL语句片段
             #  将table列表中的元素用逗号连接起来，组成table_sql语句中的字段名称和字段类型片段，用来创建表。
             #print(table)
             tables = ','.join(table) 
            
              
              #将field列表中的元素用逗号连接起来，组成insert_sql语句中的字段名称片段，用来插入数据。
             fields = ','.join(field) #字段名
              #print(fields)
              
              #  创建数据库表
              #  #如果数据库表已经存在，首先删除它
             cur.execute('drop table if exists {};'.format(filename)) 
             conn.commit()
              
             # 构建创建表的SQL语句
             table_sql = 'CREATE TABLE IF NOT EXISTS ' + filename + '(' + 'id int PRIMARY KEY NOT NULL auto_increment,' + tables + ');'
             #print('table_sql is: ' + table_sql)
              
             # 开始创建数据库表
             print('表:' + filename + ',开始创建…………')
             cur.execute(table_sql)
             conn.commit()
             print('表:' + filename + ',创建成功!')
              
             # 向数据库表中插入数据
             print('表:' + filename + ',开始插入数据…………')
              
             # 将数据框的数据读入列表。每行数据是一个列表，所有数据组成一个大列表。也就是列表中的列表，将来可以批量插入数据库表中。
             values = f.values.tolist() #所有的数据
             #print(values)
              
             # 计算数据框中总共有多少个字段，每个字段用一个 %s 替代。
             s = ','.join(['%s' for _ in range(len(f.columns))]) 
             #print(s)
              
             # 构建插入数据的SQL语句
             insert_sql = 'insert into {}({}) values({})'.format(filename,fields,s)
             #print('insert_sql is:' + insert_sql)
              
             # 开始插入数据
             cur.executemany(insert_sql, values) #使用 executemany批量插入数据
             conn.commit()
             print('表:' + filename + ',数据插入完成！')
             print(' ')
    print('任务完成！共导入 {} 个CSV文件。'.format(i))         
       
       
 #获取数据库中所有表的名字、数据和进行股票选择
def table_data(conn,cur):
     results =[]  #存放表名
      
     sql = "SHOW TABLES FROM test" #获取全部表名
     cur.execute(sql) #查询表的名单
     result = cur.fetchall() #获取表名的全部数据
     for i in range(len(result)):
         results.append((result[i][0])) #元组换为列表
     #print(results)
     
     name_of_shares = input("输入股票代号，如:（000858）：")#股票选择
    
     #构建表的名字供后期SQL语句调用
     name_of_shares.split('.')[0]
     name_of_shares = 'tab_' + name_of_shares 
     #print(name_of_shares)
     
    
     
    #获取数据库表的列名
     cur.execute('select * from '+name_of_shares)
 
     result = cur.fetchall()  # 获取查询结果
     col_result = cur.description  # 获取查询结果的字段描述
 
     columns = []
     for i in range(len(col_result)):
         columns.append(col_result[i][0])  # 获取字段名，以列表形式保存
 
    #print(columns)
     
     
     #判断该表是否在数据库存在
     if(name_of_shares in results) :
         name_of_shares='select * from  '+ name_of_shares   #sql语句拼接
         print(name_of_shares) 
         print('数据链接中...')              
         #创建游标
         cur.execute(name_of_shares)
         #获取所有数据
         result=cur.fetchall()
        #创建带列名dataframe与数据库表的数据拼接
         df_result=pd.DataFrame(list(result),columns=columns)
         #df_result['日期'] = df_result.astype('datetime64[ns]') # date转为时间格式
         print("数据获取成功！")
         
         return df_result
     
     else:
         print("该表不存在!")
       
     

    

def choose_shares():#股票选择函数(从文件中提取数据)
    name_of_shares = input("输入股票代号，如:（000858）：")
    #if  name_of_shares =='五粮液' or name_of_shares=='000858':
    try:
        for csv in csvs:
            if name_of_shares in csv:
                df = pd.read_csv(csv,encoding= "gbk",parse_dates=['日期'])#将【日期】列数据读取为时间类型
                return df
    except:
        print('error!')
 
        
 
#判断文件是否存在决定数据写入方式   
def write_to_csv(pd):           
   
    if os.path.exists('E:\python\物联网云端数据处理实验-5-2018119161-廖瑞金\output.csv'):
        try:
            pd.to_csv('E:\python\物联网云端数据处理实验-5-2018119161-廖瑞金\output.csv', mode='a',index=False,encoding= "gbk",header=False)
            #print(pd)
            print('Successful writing!')
            time.sleep(3)
        except:
            print('error')
    else:
        try:
            pd.to_csv('E:\python\物联网云端数据处理实验-5-2018119161-廖瑞金\output.csv', mode='a',encoding= "gbk",index=False)
            #print(pd)
            print('Successful writing!')
            time.sleep(3)
        except:
            print('error!')
def df_is_true_or_not(pd):
    #判断是否成功读取数据
    if pd is None:      
        print('输入有误!')
        time.sleep(3)
    else:
        return True

def is_same_or_not(df1,df2):
    df1_r, df1_c = df1.shape
    df2_r, df2_c = df2.shape
    if ((df1_r != df2_r) or (df1_c != df2_c)) :
        return False
    df1_t = df1.fillna(value='')
    df2_t = df2.fillna(value='')
    res = list(df1_t.values.ravel() == df2_t.values.ravel()) 
    return res.count(False) <= 0

def menu():
    print('*******************************欢迎使用数据检索功能***********************************\n')
    print("1、显示某个输入的日期的当日数据                    2、显示收盘价高于某个输入的价格的所有日期\n")
    print("3、显示股价波动高于某个输入的值的所有日期          4、显示成交量高于某个输入的值的所有日期\n")
    print("5、显示成交额高于某个输入的值的所有日期            6、显示并比较这两只股票在某个输入的日期的交易数据\n")
    print("7、显示这两只股票涨跌幅分别高于某个输入值的日期数  8、显示这两只股票成交量分别高于某个输入值的日期数\n")
    print("9、显示这两只股票成交金额分别高于某个输入值的日期数0、退出系统\n")
    try:
        num= int(input("输入数字："))
    except:
        print('请输入正确数字！')
        time.sleep(3)
    else:
        
        if num == 1:
            function1(num)
        if num == 2:
            function2(num)
        if num == 3:
            function3(num)
        if num == 4:
            function4(num)
        if num == 5:
            function5(num)
        if num == 6:
            function6(num)
        if num == 7:
            function7(num)
        if num == 8:
            function8(num)
        if num == 9:
            function9(num)
        if num == 0:
            quit()
    menu()

#选择一只股票，显示此股票在某个输入的日期的当日数据 
def function1(num):
     df = table_data(conn,cur)
     if df_is_true_or_not(df):
        date = input('输入日期：（年-月-日）:')
        #判断数据是否为空
        if df[df['日期']==date].empty:
            print('无数据！')
            print('请输入其他日期')
        else:
            print(df[df['日期']==date])
            #写入数据
            write_to_csv(df[df['日期']==date])
            
#选择一只股票，显示此股票收盘价高于某个输入的价格的所有日期
def function2(num):
    df = table_data(conn,cur)
    if df_is_true_or_not(df):
        try:
            date = float(input('输入收盘价:'))
            if df[df['收盘价']>date].empty:
                print('无数据')
            else:
                print(df[df['收盘价']>date])
                #写入数据
                write_to_csv(df[df['收盘价']>date])
        except :
            print('输入有误！')
            time.sleep(3)

#选择一只股票，显示此股票股价涨跌幅高于某个输入值的所有日期
def function3(num):
    df = table_data(conn,cur)
    if df_is_true_or_not(df):
        date = input('输入涨跌幅:')
        if df[df['涨跌幅']>date].empty:
          print('无数据')
        else:
            #剔除无关数据
            df = df[~df['涨跌幅'].isin(['None'])]
            print(df[df['涨跌幅']>date])
            #写入数据
            write_to_csv(df[df['涨跌幅']>date]) 

#选择一只股票，显示此股票成交量高于某个输入的值的所有日期
def function4(num):
    df = table_data(conn,cur)
    if df_is_true_or_not(df):
        try:
            date = int(input('成交量:'))
            if df[df['成交量']>date].empty:
                print('无数据')
            else:
                print(df[df['成交量']>date])
                #写入数据
                write_to_csv(df[df['成交量']>date])
        except:
            print('输入有误！')
            time.sleep(3)

#选择一只股票，显示此股票成交金额高于某个输入的值的所有日期
def function5(num):
    df = table_data(conn,cur)
    if df_is_true_or_not(df):
        try:
            date = float(input('输入成交金额:'))
            if df[df['成交金额']>date].empty:
                print('无数据')
                time.sleep(3)
            else:
                print(df[df['成交金额']>date])
                #写入数据
                write_to_csv(df[df['成交金额']>date])
        except :
            print('输入有误！')
            time.sleep(3)

#选择两只股票，显示并比较这两只股票在某个输入的日期的交易数据
def function6(num):
    print('输入第一个股票：')
    df1 = table_data(conn,cur)
    print('输入第二个股票：')
    df2 = table_data(conn,cur)
    if df1 is None or df2 is None:
        print('输入有误!')
        time.sleep(3)
        #判断是否同一个股票
    elif(is_same_or_not(df1,df2)):
        print('请输入不同的股票！')
    else:
        try:
            date = input('输入日期（年-月-日）：')
            if df1[df1['日期']==date].empty or df2[df2['日期']==date].empty:
                if df1[df1['日期']==date].empty :
                    print(df1.iloc[[1],[2]],'该日期无数据！')
                if df2[df2['日期']==date].empty :
                    print(df2.iloc[[1],[2]],'该日期无数据！')
                if df1[df1['日期']==date].empty and df2[df2['日期']==date].empty:
                    print('无数据！')
                time.sleep(3)
            else:
                print(df1[df1['日期']==date])
                write_to_csv(df1[df1['日期']==date])
                print(df2[df2['日期']==date])
                write_to_csv(df2[df2['日期']==date])
        except :
            print('输入有误！')
            time.sleep(3)

#选择两只股票，显示这两只股票涨跌幅分别高于某个输入值的日期数
def function7(num):
    print('输入第一个股票：')
    df1 = table_data(conn,cur)
    print('输入第二个股票：')
    df2 = table_data(conn,cur)
    if df1 is None or df2 is None:
        print('输入有误!')
        time.sleep(3)
    #判断是否同一个股票
    elif(is_same_or_not(df1,df2)):
        print('请输入不同的股票！')
    else:
        try:
            price = input('输入涨跌幅：')
            if df1[df1['涨跌幅']>price].empty or df2[df2['涨跌幅']>price].empty:
                if df1[df1['涨跌幅']>price].empty and df2[df2['涨跌幅']>price].empty:
                    print('无数据')
                if df1[df1['涨跌幅']>price].empty :
                    print(df1.iloc[[1],[2]],'无数据！')
                if df2[df2['涨跌幅']>price].empty :
                    print(df2.iloc[[1],[2]],'无数据！') 
                    time.sleep(3)
            else:
                #剔除无关数据
                df1 = df1[~df1['涨跌幅'].isin(['None'])]
                print(df1.iloc[[1],[2]],'日期数',len(df1[df1['涨跌幅']>price]))
                write_to_csv(df1[df1['涨跌幅']>price])
                #剔除无关数据
                df2 = df2[~df2['涨跌幅'].isin(['None'])]
                print(df2.iloc[[1],[2]],'日期数',len(df2[df2['涨跌幅']>price]))
                write_to_csv(df2[df2['涨跌幅']>price])
        except :
            print('输入有误！')
            time.sleep(3)
#选择两只股票，显示这两只股票成交量分别高于某个输入值的日期数
def function8(num):
    print('输入第一个股票：')
    df1 = table_data(conn,cur)
    print('输入第二个股票：')
    df2 = table_data(conn,cur)
    if df1 is None or df2 is None:
        print('输入有误!')
        time.sleep(3)
    #判断是否同一个股票
    elif(is_same_or_not(df1,df2)):
        print('请输入不同的股票！')
    else:
        try:
            price = int(input('输入成交量：'))
            if df1[df1['成交量']>price].empty or df2[df2['成交量']>price].empty:
                if df1[df1['成交量']>price].empty and df2[df2['成交量']>price].empty:
                    print('无法比较！')
                if df1[df1['成交量']>price].empty :
                    print(choose_shares().name_of_shares+'无数据！')
                if df2[df2['成交量']>price].empty :
                    print(choose_shares().name_of_shares+'无数据！')
                time.sleep(3)
            else:
                print(df1.iloc[[1],[2]],'日期数',len(df1[df1.成交量>price]))
                write_to_csv(df1[df1['成交量']>price])
                print(df2.iloc[[1],[2]],'日期数',len(df2[df2.成交量>price]))
                write_to_csv(df2[df2['成交量']>price])
        except :
            print('输入有误！')
            time.sleep(3)

#选择两只股票，显示这两只股票成交金额分别高于某个输入值的日期数
def function9(num):
    print('输入第一个股票：')
    df1 = table_data(conn,cur)
    print('输入第二个股票：')
    df2 = table_data(conn,cur)
    if df1 is None or df2 is None:
        print('输入有误!')
        time.sleep(3)
     #判断是否同一个股票
    elif(is_same_or_not(df1,df2)):
        print('请输入不同的股票！')
    else:
        try:
            price = float(input('输入成交金额：'))
            print(price)
            if df1[df1['成交金额']>price].empty or df2[df2['成交金额']>price].empty:
                print(price)
                if df1[df1['成交金额']>price].empty :
                    print(df1.iloc[[1],[2]],'无数据！')
                if df2[df2['成交金额']>price].empty :
                    print(df2.iloc[[1],[2]],'无数据！')
                if df1[df1['成交金额']>price].empty and df2[df2['成交金额']>price].empty:
                    print('无数据！')
                time.sleep(3)
            else:
                print(df1.iloc[[1],[2]] , '日期数' , len(df1[df1.iloc[:,12]>price]))
                write_to_csv(df1[df1['成交金额']>price])
                print(df2.iloc[[1],[2]],'日期数',len(df2[df2.iloc[:,12]>price]))
                write_to_csv(df2[df2['成交金额']>price])
        except:
            print('输入有误！')
            time.sleep(3)         
        
#load_csv(conn,cur)
menu()