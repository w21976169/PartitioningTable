using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wynnyo.PartitioningTable.Entities;
using Wynnyo.PartitioningTable.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Wynnyo.PartitioningTable.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LogController : ControllerBase
    {
        private readonly LogService _service;

        public LogController(LogService service)
        {
            _service = service;
        }


        [HttpPost]
        [Route("insert")]
        public void Insert(LogEntity log)
        {
            try
            {
                _service.Insert(log);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpPost]
        [Route("rangeInsert")]
        public void RangeInsert(int rows)
        {
            var ran = new Random();

            var list = new List<LogEntity>();

            try
            {
                for (int i = 0; i < rows; i++)
                {
                    // 随机生成 分区范围内的 时间
                    var time = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day,
                            ran.Next(0, 24), ran.Next(0, 60), ran.Next(0, 60))
                        .AddDays(0 - ran.Next(0, Consts.ReserveDay));

                    list.Add(new LogEntity()
                    {
                        Id = 0,
                        Title = "Log",
                        CreateTime = time
                    });
                }

                _service.Insert(list);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}