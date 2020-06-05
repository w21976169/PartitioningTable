using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlSugar;

namespace Wynnyo.PartitioningTable.Entities
{
    public class LogEntity
    {
        [SugarColumn(IsNullable = false, IsIdentity = true, IsPrimaryKey = true)]
        public int Id { get; set; }

        [SugarColumn(IsNullable = false)]
        public string Title { get; set; }

        [SugarColumn(IsNullable = false)]
        public DateTime CreateTime { get; set; }
    }
}
