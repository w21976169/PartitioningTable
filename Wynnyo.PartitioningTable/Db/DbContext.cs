using System;
using System.Linq;
using SqlSugar;

namespace Wynnyo.PartitioningTable.Db
{
    public class DbContext
    {
        public SqlSugarClient Db;
        public DbContext()
        {
            Db = new SqlSugarClient(new ConnectionConfig()
            {
                ConnectionString = "Server=.;Database=MyData;Trusted_Connection=True;MultipleActiveResultSets=true",  //定义数据库路径，可以写入配置文件再读取，偷懒直接这样写。
                DbType = DbType.SqlServer, //指定数据库类型

                InitKeyType = InitKeyType.Attribute, //从实体特性中读取主键自增列信息
                IsAutoCloseConnection = true //是否自动关闭连接
            });

            //用来打印Sql方便你调式    
            Db.Aop.OnLogExecuting = (sql, pars) =>
            {
                Console.WriteLine(sql + "\r\n" +
                                  Db.Utilities.SerializeObject(pars.ToDictionary(it => it.ParameterName, it => it.Value)));
                Console.WriteLine();
            };
        }
    }
}
