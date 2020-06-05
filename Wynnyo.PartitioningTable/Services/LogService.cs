using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wynnyo.PartitioningTable.Db;
using Wynnyo.PartitioningTable.Entities;

namespace Wynnyo.PartitioningTable.Services
{
    public class LogService
    {
        public readonly DbContext _dbContext;
        public LogService()
        {
            _dbContext = new DbContext();
        }

        /// <summary>
        /// 插入单条数据
        /// </summary>
        /// <param name="log"></param>
        public void Insert(LogEntity log)
        {
            _dbContext.Db.MappingTables.Add("LogEntity", Consts.TableName);
            _dbContext.Db.Insertable(log).ExecuteCommand();
        }

        /// <summary>
        /// 插入多条数据
        /// </summary>
        /// <param name="logs"></param>
        public void Insert(ICollection<LogEntity> logs)
        {
            _dbContext.Db.MappingTables.Add("LogEntity", Consts.TableName);
            _dbContext.Db.Insertable(logs.ToList()).ExecuteCommand();
        }
    }
}
