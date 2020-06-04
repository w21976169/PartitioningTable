

# SqlServer自动化表分区解决方案



## 背景

在实际的业务当中, Log日志的**增长速度非常快**, 而查询报表基本都是看**最近的记录**. 为兼顾性能和业务的需要, 日志表需要**定时(t1)的做一个分区**, 并且仅**保留最近一段时间内(t2)**的数据.需要注意的是, **t2最好为t1的整数倍**. 

## 介绍

- 分区表是把数据按设定的标准划分成区域存储在不同的文件组中;
- 表分区可以分为: 水平分区 (行级) 和 垂直分区 (列级), 本业务中使用 水平分区;
- 优点: 
  - 改善查询性能：对分区对象的查询可以仅搜索自己关心的分区，提高检索速度。
  - 增强可用性：如果表的某个分区出现故障，表在其他分区的数据仍然可用；
  - 维护方便：如果表的某个分区出现故障，需要修复数据，只修复该分区即可；
  - 均衡I/O：可以把不同的分区映射到磁盘以平衡I/O，改善整个系统性能。
- 缺点:
  - 分区太多, 会增加数据库对分区表扫描的消耗;



## 本业务中需要解决的问题

- 根据实际业务和需求, 调研具体的 t1 和 t2 的时间;

- 在不停机维护下, 动态增加分区表;

- 清理以前的数据时, 需要把以前的分区一并回收, 从而减少数据库对分区扫描的消耗;


## 解决方案

- 本系统为 Saas 系统, Log 增长速度较快, 把 t1 暂定为 1 天;
- 保留时间 t2, 暂定为 31 天;
- 测试数据库为: MyData;
- 测试表名为: Log;

### 示意图

- 分区一共有 34 个, 其中1个索引分区, 31个主分区, 2个预留分区;
- 索引分区 为初始分区, 主要是用来清除历史数据, 合并分区用, 这个分区一直存在;
- 主分区是用来 存储数据的分区, 31个可以满足一个月的数据保留;
- 预留分区是方便扩容分区,不必每天 0 点来操作, 多个分区可以防止当天 扩容任务没有成功;

![Snipaste_2020-06-04_13-16-09](http://wynnyo.com/upload/2020/06/Snipaste_2020-06-04_13-16-09-84e73a710d9e472f8b5743bf716f4ca4.png)

![Snipaste_2020-06-04_13-16-42](http://wynnyo.com/upload/2020/06/Snipaste_2020-06-04_13-16-42-5bad10f171aa4ee28a643007144e99dd.png)

![Snipaste_2020-06-04_13-46-22](http://wynnyo.com/upload/2020/06/Snipaste_2020-06-04_13-46-22-1e75b393dd2d48cdb191e1ccf94ef535.png)

### 初始化分区

- 新建 文件和文件组, 一共 34 个;
- 创建分区函数和方案;
- 为 Log 表绑定该分区方案;

### 每天定时任务

- 增加预留分区-查看系统中是否存在, 不存在增加
- 删除超过31天的分区, 需要把文件和文件组删除
- 修改 分区函数 和 分区方案



## 项目示例

### 新建数据库

新建数据库 MyData

### 新建项目

创建 .net core api 项目: Wynnyo.PartitioningTable

### 链接数据库

- 安装 SqlSugarCore nuget包, 不会的可以直接去[官网](http://www.codeisbug.com/Doc/8)查看;
- 安装 Swagger 方便调试, 参考 [微软官方文档](https://docs.microsoft.com/en-us/aspnet/core/tutorials/getting-started-with-swashbuckle?view=aspnetcore-3.1&tabs=visual-studio);
- 添加 Db 文件夹, 创建 DbContext.cs 文件;
	```c#
	public class DbContext
	{
		public SqlSugarClient Db;
		public DbContext()
		{
			Db = new SqlSugarClient(new ConnectionConfig()
			{
 				//定义数据库路径，可以写入配置文件再读取，偷懒直接这样写。
				ConnectionString = "Server=.;Database=MyData;Trusted_Connection=True;MultipleActiveResultSets=true", 
				DbType = DbType.SqlServer, //指定数据库类型
				InitKeyType = InitKeyType.Attribute, //从实体特性中读取主键自增列信息
				IsAutoCloseConnection = true //是否自动关闭连接
			});
      
			//用来打印Sql方便你调式    
			Db.Aop.OnLogExecuting = (sql, pars) =>
        	{
        	};
		}
	}
	```

### 初始化分区

初始化分区需要预留 1 到 2 个分区, 这样既可以防止当天分区创建失败, 又可以避免数据库必须在 0 点创建分区.

- 新建 Consts.cs 文件， 定义常量；
  ```c#
  public static class Consts()
  {
	    public static string DbName = "MyData"; // 数据库的 name
	    public static string TableName = "Log";  // 表的 name
	    public static string FilePath = "C:\\MyData"; // 分区表存储的文件夹
	    public static string FileName = "Log_Data_"; // 分区表存储的文件夹
	    public static int FileSize = 5; // 文件初始大小(MB)
	    public static int FileMaxSize = 100; // 文件初始大小(MB)
  
      public static string PartitionFunctionName = "F_Date_Day"; // 分区函数名称
      public static string PartitionSchemeName = "P_Date_Day"; // 分区方案名称
  
      public static int TaskDay = 1; // 自动增加分区的时间为 1 天
      public static int ReserveDay = 31; // 预留数据为31天, 这样可以显示完整的一个月数据
  
      public static int ReservePartitions = 2; // 预分区数量
  }
  ```
  
- 新建 Services 文件夹 和 DbService.cs 文件；

  ```c#
  public readonly DbContext _dbContext;
  public DbService()
  {
		_dbContext = new DbContext();
  }
  ```

- 在 Startup.cs 中 注入 DbService;

  ```C#
  public void ConfigureServices(IServiceCollection services)
  {
      ...
  	services.AddSingleton<DbService>();
  }
  ```

- 添加 LogEntity.cs 类, 用来生成 Log 表;
  ```c#
  [SugarTable("Log")]
  public class LogEntity()
  {
		[SugarColumn(IsPrimaryKey = true, IsIdentity = true)]
		public int Id { get; set; }
		public string Title { get; set; }
		public DateTime CreateTime { get; set; }
  }
  ```

- 添加方法 InitDbTables， 使用 SqlSugar 的 code first 功能生成表 Log;
  ```c#
  public void InitDbTables()
  {
		_dbContext.Db.InitTables<LogEntity>();
  }
  ```

- 添加方法 InitPartitioningTables

  ```c#
  public void InitPartitioningTables()
  {
    // 循环建立 Consts.ReserveDay / Consts.TaskDay + Consts.ReservePartitions 个分区和分区文件
      var sql = new StringBuilder();
      var partitions = Consts.ReserveDay / Consts.TaskDay;
      var today = DateTime.Today;
  
      var dateList = new List<string>();
      var tableNameList = new List<string>();
  
      for (int i = partitions + Consts.ReservePartitions + 1; i > 0 ; i--)
      {
          var dayStr = today.AddDays(1 + Consts.ReservePartitions - i).ToString("yyyyMMdd");
          // 第一个分区为 索引分区,用来以后合并分区
          if (i == partitions + Consts.ReservePartitions + 1)
          {
              dayStr = "00010101";
          }
          else
          {
              dateList.Add(dayStr);
          }
  
          var tableName = Consts.TableName + dayStr;
          var fileName = Consts.FileName + dayStr;
  
          tableNameList.Add(tableName);
  
          sql.Append($"ALTER DATABASE {Consts.DbName} ADD FILEGROUP {tableName};");
          sql.Append($@"ALTER DATABASE {Consts.DbName}   
                  ADD FILE   
                  (  
                      NAME = {fileName},  
                      FILENAME = '{Path.Combine(Consts.FilePath, fileName + ".ndf")}',  
                      SIZE = {Consts.FileSize}MB,  
                      MAXSIZE = {Consts.FileMaxSize}MB,  
                      FILEGROWTH = 5MB  
                  )
                  TO FILEGROUP {tableName};");
      }
  
      // 创建 分区函数
      sql.Append($@"CREATE PARTITION FUNCTION {Consts.PartitionFunctionName}(DATETIME)
                  AS RANGE RIGHT FOR VALUES
                  (
                     '{string.Join("','", dateList)}'
                  )");
  
      // 创建分区方案
      sql.Append($@"CREATE PARTITION SCHEME {Consts.PartitionSchemeName}
                      AS PARTITION [{Consts.PartitionFunctionName}]
                      TO ({string.Join(",", tableNameList)});");
  
  
      // 为 Log 表绑定 分区方案, 创建前需要删除其聚众索引,重新创建主键非聚集索引
      sql.Append($@"ALTER TABLE {Consts.TableName} DROP CONSTRAINT PK_{Consts.TableName}_Id;
                  ALTER TABLE {Consts.TableName}
                  ADD CONSTRAINT PK_{Consts.TableName}_Id PRIMARY KEY NONCLUSTERED (Id ASC)");
      sql.Append($@"CREATE CLUSTERED INDEX IX_CreateTime ON {Consts.TableName} (CreateTime)
                  ON {Consts.PartitionSchemeName} (CreateTime)");
  
      _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
  }
  ```

### 定时任务

- 添加定时任务方法 PartitioningTablesTask

  ```c#
  public void PartitioningTablesTask(int addDay)
  {
      // 查询数据库文件组的信息
      var dt = _dbContext.Db.Ado.GetDataTable("SELECT f.[name][filegroup] FROM sys.filegroups f");
      var list = dt.AsEnumerable()
          .Select(e => e["filegroup"].ToString()?.Replace(Consts.TableName, ""))
          .ToList();
  
      var sql = new StringBuilder();
  
      // 为了测试,直接跑明天的任务 这里的 addDay = 1
      var date = DateTime.Today.AddDays(addDay);
  
      // 新增 文件组
      for (int i = 1; i <= Consts.ReservePartitions; i++)
      {
          var dateStr = date.AddDays(i).ToString("yyyyMMdd");
          // 数据库中文件组 不存在
          if (!list.Contains(dateStr))
          {
              var tableName = Consts.TableName + dateStr;
              var fileName = Consts.FileName + dateStr;
  
              sql.Append($"ALTER DATABASE {Consts.DbName} ADD FILEGROUP {tableName};");
              sql.Append($@"ALTER DATABASE {Consts.DbName}   
                  ADD FILE   
                  (  
                      NAME = {fileName},  
                      FILENAME = '{Path.Combine(Consts.FilePath, fileName + ".ndf")}',  
                      SIZE = {Consts.FileSize}MB,  
                      MAXSIZE = {Consts.FileMaxSize}MB,  
                      FILEGROWTH = 5MB  
                  )
                  TO FILEGROUP {tableName};");
  
              // 新增分区函数和分区方案
              sql.Append($"ALTER PARTITION SCHEME {Consts.PartitionSchemeName} NEXT USED {tableName}; ");
              sql.Append($"ALTER PARTITION FUNCTION {Consts.PartitionFunctionName} () SPLIT RANGE('{dateStr}'); ");
  
          }
      }
  
      if (!string.IsNullOrWhiteSpace(sql.ToString()))
      {
          _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
      }
  
      sql.Clear();
      // 删除以前的文件组
  
      var deleteDate = date.AddDays(0 - Consts.ReserveDay / Consts.TaskDay).ToString("yyyyMMdd");
      // 数据库中文件组 存在
      if (list.Contains(deleteDate))
      {
          var fileName = Consts.FileName + deleteDate;
          var tableName = Consts.TableName + deleteDate;
  
          sql.Append($@"ALTER DATABASE {Consts.DbName} REMOVE FILE {fileName};");
  
          // 合并分区
          sql.Append($"ALTER PARTITION FUNCTION {Consts.PartitionFunctionName} () MERGE RANGE('{deleteDate}');");
  
          sql.Append($@"ALTER DATABASE {Consts.DbName} REMOVE FILEGROUP {tableName};");
      }
  
      if (!string.IsNullOrWhiteSpace(sql.ToString()))
      {
          _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
      }
  }
  ```

  