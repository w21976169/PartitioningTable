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


        public void Insert(LogEntity log)
        {
            _dbContext.Db.Insertable(log).ExecuteCommand();
        }

        public void Insert(ICollection<LogEntity> logs)
        {
            _dbContext.Db.Insertable(logs.ToList()).ExecuteCommand();
        }
    }
}
