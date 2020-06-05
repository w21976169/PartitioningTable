using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using Wynnyo.PartitioningTable.Db;
using Wynnyo.PartitioningTable.Entities;

namespace Wynnyo.PartitioningTable.Services
{
    public class DbService
    {
        public readonly DbContext _dbContext;

        public DbService()
        {
            _dbContext = new DbContext();
        }

        /// <summary>
        /// 初始化 Log 表
        /// </summary>
        public void Init()
        {
            // 生成 Log 表
            _dbContext.Db.MappingTables.Add("LogEntity", Consts.TableName );
            _dbContext.Db.CodeFirst.InitTables<LogEntity>();

            // 创建必要的目录
            if (!Directory.Exists(Consts.FilePath))
                Directory.CreateDirectory(Consts.FilePath);

            if (!Directory.Exists(Consts.XmlBakFilePath))
                Directory.CreateDirectory(Consts.XmlBakFilePath);
        }


        /// <summary>
        /// 初始化分区并绑定表
        /// </summary>
        public void InitPartitioningTables()
        {
            // 循环建立 Consts.ReserveDay / Consts.TaskDay + Consts.ReservePartitions + 1 个分区和分区文件
            var sql = new StringBuilder();
            var partitions = Consts.ReserveDay / Consts.TaskDay;
            var today = DateTime.Today;

            var dateList = new List<string>();
            var tableNameList = new List<string>();

            for (int i = partitions + Consts.ReservePartitions + 1; i > 0; i--)
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
            sql.Append($@"ALTER TABLE {Consts.TableName} DROP CONSTRAINT PK_{Consts.TableName}_Id;");
            sql.Append($@"CREATE CLUSTERED INDEX IX_CreateTime ON {Consts.TableName} (CreateTime)
                        ON {Consts.PartitionSchemeName} (CreateTime);");

            _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
        }

        /// <summary>
        /// 每天定时任务, 动态操作分区, 动态 修改 分区函数, 动态修改分区方案
        /// </summary>
        public void PartitioningTablesTask(int addDay = 0)
        {
            // 查询数据库文件组的信息
            var dt = _dbContext.Db.Ado.GetDataTable("SELECT f.[name][filegroup] FROM sys.filegroups f");
            var list = dt.AsEnumerable()
                //.Where(e => !string.IsNullOrWhiteSpace(e["name"]?.ToString()) &&
                //            e["name"].ToString().StartsWith("Consts.FileName"))
                .Select(e => e["filegroup"].ToString()?.Replace(Consts.TableName, ""))
                .ToList();

            var sql = new StringBuilder();

            // 为了测试,直接跑明天的任务
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
                    sql.Append(
                        $"ALTER PARTITION FUNCTION {Consts.PartitionFunctionName} () SPLIT RANGE('{dateStr}'); ");

                    if (!string.IsNullOrWhiteSpace(sql.ToString()))
                    {
                        _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
                        sql.Clear();
                    }

                }
            }


            // 删除以前的文件组

            var deleteDate = date.AddDays(0 - Consts.ReserveDay / Consts.TaskDay).ToString("yyyyMMdd");
            // 数据库中文件组 存在
            if (list.Contains(deleteDate))
            {
                var fileName = Consts.FileName + deleteDate;
                var tableName = Consts.TableName + deleteDate;

                // 这里需要创建临时表, 移动数据后, 把临时表删除
                _dbContext.Db.MappingTables.Add("LogEntity", Consts.TempTableName);
                _dbContext.Db.CodeFirst.InitTables<LogEntity>();

                // 为 Log 表绑定 分区方案, 创建前需要删除其聚众索引,重新创建主键非聚集索引
                sql.Append($@"ALTER TABLE {Consts.TableName}_temp DROP CONSTRAINT PK_{Consts.TempTableName}_Id;");
                sql.Append($@"CREATE CLUSTERED INDEX IX_CreateTime ON {Consts.TempTableName} (CreateTime)
                        ON {tableName};");

                // 将分区表上对应分区数据移动到临时表中
                sql.Append($"ALTER TABLE {Consts.TableName} SWITCH PARTITION 2 to {Consts.TempTableName};");

                // 利用 bcp 和 xp_cmdshell 把 sql data 保存为 xml 文件
                sql.Append(
                    "EXEC xp_cmdshell 'bcp " +
                    $"\"SELECT * FROM [{Consts.DbName}].[dbo].[{Consts.TempTableName}] FOR XML PATH(''Row'')," + 
                    $" ROOT(''Log_{deleteDate}'')\" queryout \"{Path.Combine(Consts.XmlBakFilePath, fileName + ".xml")}\" -T -c'");

                // 删除临时表
                sql.Append($"TRUNCATE TABLE {Consts.TempTableName};");
                sql.Append($"DROP TABLE {Consts.TempTableName};");

                // 合并分区
                sql.Append($"ALTER PARTITION FUNCTION {Consts.PartitionFunctionName} () MERGE RANGE('{deleteDate}');");

                // 移除文件文件组
                sql.Append($@"ALTER DATABASE {Consts.DbName} REMOVE FILE {fileName};");
                sql.Append($@"ALTER DATABASE {Consts.DbName} REMOVE FILEGROUP {tableName};");

                if (!string.IsNullOrWhiteSpace(sql.ToString()))
                {
                    _dbContext.Db.Ado.ExecuteCommand(sql.ToString());
                    sql.Clear();
                }
            }
        }


        /// <summary>
        /// 统计文件和文件组信息
        /// </summary>
        /// <returns></returns>
        public DataTable GetGroupFileInfo()
        {
            var sql = @"SELECT df.[name], df.physical_name, f.[name][filegroup]
                      FROM sys.database_files df
                      JOIN sys.filegroups f ON df.data_space_id = f.data_space_id";
            return _dbContext.Db.Ado.GetDataTable(sql);
        }

        /// <summary>
        /// 统计分区数据信息
        /// </summary>
        /// <returns></returns>
        public DataTable GetPartitionsInfo()
        {
            var sql = $@"SELECT PARTITION = $PARTITION.{Consts.PartitionFunctionName} (createtime),
                               ROWS      = COUNT(*),
                               MinVal    = MIN(createtime),
                               MaxVal    = MAX(createtime)
                        FROM [dbo].[Log]
                        GROUP BY $PARTITION.{Consts.PartitionFunctionName} (createtime)
                        ORDER BY PARTITION";
            return _dbContext.Db.Ado.GetDataTable(sql);
        }
    }
}