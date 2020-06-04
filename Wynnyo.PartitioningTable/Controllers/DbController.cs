using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Wynnyo.PartitioningTable.Dtos;
using Wynnyo.PartitioningTable.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Wynnyo.PartitioningTable.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DbController : ControllerBase
    {
        private readonly DbService _dbService;
        public DbController(DbService dbService)
        {
            _dbService = dbService;
        }

        [HttpPost]
        [Route("initTable")]
        public void InitDbTable()
        {
            try
            {
                _dbService.InitDbTable();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpPost]
        [Route("initPartitioningTables")]
        public void InitPartitioningTables()
        {
            try
            {
                _dbService.InitPartitioningTables();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpPost]
        [Route("partitioningTablesTask")]
        public void PartitioningTablesTask(int addDay = 0)
        {
            try
            {
                _dbService.PartitioningTablesTask(addDay);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpGet]
        [Route("getPartitioningTablesInfo")]
        public ICollection<DbInfoDto> GetPartitioningTablesInfo()
        {
            try
            {
                var dt = _dbService.GetPartitioningTablesInfo();

                return dt.AsEnumerable().Select(e => new DbInfoDto
                {
                    Partition = e["PARTITION"].ToString(),
                    Rows = e["ROWS"].ToString(),
                    MinVal = e["MinVal"].ToString(),
                    MaxVal = e["MaxVal"].ToString(),

                }).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpGet]
        public string Get()
        {
            var list = new List<string>();
            var initTime = new DateTime(2020,6,1);
            for (int i = 0; i < 32; i++)
            {
                list.Add(initTime.AddDays(0 - i).ToString("yyyy-MM-dd"));
            }

            return string.Join("','", list);
        }

    }
}