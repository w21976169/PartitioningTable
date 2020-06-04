using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wynnyo.PartitioningTable.Dtos
{
    public class DbInfoDto
    {
        public string Partition { get; set; }
        public string Rows { get; set; }
        public string MinVal { get; set; }
        public string MaxVal { get; set; }
    }
}
