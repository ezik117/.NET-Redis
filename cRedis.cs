using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Text.RegularExpressions;

namespace Redis
{
    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Класс для возврата данных с сервера Redis
    /// 1) Поле Status возвращает True, если данные были успешно обработаны
    /// и False в случае ошибки обмена данными
    /// 2) В поле Result содержится текст ошибки как строка, если статус равен False, или 
    /// результат выполнения команды Redis
    /// 3) Если значение из Redis равно NULL, то на выходе будет: Status=True, Result=null
    /// 4) Значение Result может быть null, long, string
    /// </summary>
    public class cResult
    {
        /// <summary>
        /// Результат выполнения команды или строка с ошибкой, если status=false
        /// </summary>
        public object result;

        /// <summary>
        /// Статус выполнения команды.True-если успешно.
        /// </summary>
        public bool status;

        public cResult(bool status, object result)
        {
            this.status = status;
            this.result = result;
        }
    }

    // --------------------------------------------------------------------------------------------
    /// <summary>
    /// Класс реализующий базовый протокол передачи данных с сервером Redis по протоколу TCP/IP
    /// </summary>
    class cRedis
    {

        private string RedisServerIP;
        private int RedisServerPort;
        private TcpClient redis = null;
        private bool KeepAlive;

        /// <summary>
        /// Набор некоторых стандартных функций.
        /// Работает только в режиме когда KeepAlive = true.
        /// </summary>
        public cSomeStandardFunctions funcs;


        /// <summary>
        /// Конструктор. Создает объект Redis.
        /// </summary>
        /// <param name="RedisServerIP">Строковое имя или IP адрес сервера Redis</param>
        /// <param name="RedisServerPort">Порт сервера Redis</param>
        /// <param name="KeepAlive">Если true - пытается сооздать постоянное подключение к серверу. 
        /// Иначе подключение создается каждый раз при вызове команды.</param>
        public cRedis(string RedisServerIP = "localhost", int RedisServerPort = 6379, bool KeepAlive=false)
        {
            this.KeepAlive = KeepAlive;
            this.RedisServerPort = RedisServerPort;
            this.RedisServerIP = RedisServerIP;
            funcs = new cSomeStandardFunctions(this);
            if (KeepAlive)
            {
                redis = new TcpClient();
                redis.ReceiveTimeout = redis.SendTimeout = 3000;
                try
                {
                    redis.Connect(this.RedisServerIP, this.RedisServerPort);
                }
                catch
                {
                    redis = null;
                }
            }
        }


        /// <summary>
        /// Если класс cRedis был вызван с KeepAlive = true, корректно закрывает соединение с сервером Redis.
        /// </summary>
        public void Close()
        {
            try
            {
                redis?.Close();
                redis?.Dispose();
                redis = null;
            }
            catch
            { }
        }

        /// <summary>
        /// Возвращает состояние соединения с сервером Redis. 
        /// </summary>
        /// <returns></returns>
        public bool Connected()
        {
            if (redis == null) return false;
            return redis.Connected;
        }

        /// <summary>
        /// Отправляет команду на сервер Redis. Команда соответствует формату Redis CLI
        /// Если при инициализации класса cRedis параметр KeepAlive был не установлен, то при
        /// отправке команды происходит одноразовое подключение по TCP/IP. Иначе используется существующее
        /// подключение.
        /// </summary>
        /// <param name="command">Команда</param>
        /// <returns>Класс cResult</returns>
        public cResult SendCommand(string command)
        {
            NetworkStream nt;

            if (this.redis == null)
            {
                // Если нет подключения, подключимся
                try
                {
                    redis = new TcpClient();
                    redis.ReceiveTimeout = redis.SendTimeout = 3000;
                    redis.Connect(this.RedisServerIP, this.RedisServerPort);
                }
                catch (Exception e)
                {
                    redis = null;
                    return new cResult(false, e.Message);
                }
            }
            else
            {
                if (!redis.Connected && KeepAlive)
                {
                    // Переподключимся, если соединение потеряно
                    try
                    {
                        redis = new TcpClient();
                        redis.ReceiveTimeout = redis.SendTimeout = 3000;
                        redis.Connect(this.RedisServerIP, this.RedisServerPort);
                    }
                    catch (Exception e)
                    {
                        redis = null;
                        return new cResult(false, e.Message);
                    }
                }
            }

            // преобразуем в формат RESP
            string resp = ConvertToRESP(command);

            // отправляем
            try
            {
                Byte[] data = System.Text.Encoding.UTF8.GetBytes(resp);
                nt = redis.GetStream();
                nt.Flush();
                nt.Write(data, 0, data.Length);
            }
            catch (Exception e)
            {
                if (!KeepAlive)
                {
                    redis?.Close();
                    redis?.Dispose();
                    redis = null;
                }
                return new cResult(false, e.Message);
            }

            // получаем ответ, отключаемся, парсим и возвращаем ответ
            try
            {
                byte[] bytes = new byte[redis.ReceiveBufferSize];
                int bytesRead = nt.Read(bytes, 0, redis.ReceiveBufferSize);

                string[] msg = Encoding.UTF8.GetString(bytes, 0, bytesRead).Split(new string[] { "\r\n" }, StringSplitOptions.None);
                if ((this.redis == null) && (redis.Connected)) redis.Close();

                if (msg[0].Length != 0)
                {
                    // тип Simple String
                    if (msg[0][0] == '+')
                    {
                        if (!KeepAlive)
                        {
                            redis?.Close();
                            redis?.Dispose();
                            redis = null;
                        }
                        return new cResult(true, msg[0].Remove(0,1));
                    }
                    // тип Integers
                    else if (msg[0][0] == ':')
                    {
                        if (!KeepAlive)
                        {
                            redis?.Close();
                            redis?.Dispose();
                            redis = null;
                        }
                        return new cResult(true, Convert.ToInt64(msg[0].Remove(0, 1)));
                    }
                    // тип Bulk Strings
                    else if (msg[0][0] == '$')
                    {
                        // получаем количество символов в строке
                        int strLen;
                        if (int.TryParse(msg[0].Remove(0, 1), out strLen))
                        {
                            // нет строки
                            if (strLen < 0)
                            {
                                if (!KeepAlive)
                                {
                                    redis?.Close();
                                    redis?.Dispose();
                                    redis = null;
                                }
                                return new cResult(true, null);
                            }
                            // пустая строка
                            else if (strLen == 0)
                            {
                                if (!KeepAlive)
                                {
                                    redis?.Close();
                                    redis?.Dispose();
                                    redis = null;
                                }
                                return new cResult(true, "");
                            }
                            // строка
                            else
                            {
                                if (!KeepAlive)
                                {
                                    redis?.Close();
                                    redis?.Dispose();
                                    redis = null;
                                }
                                return new cResult(true, msg[1]);
                            }
                        }
                        else
                        {
                            // что то пошло не так
                            if (!KeepAlive)
                            {
                                redis?.Close();
                                redis?.Dispose();
                                redis = null;
                            }
                            return new cResult(false, "Internal error of parsing length of BulkStrings");
                        }
                    }
                    // тип Error
                    else if (msg[0][0] == '-')
                    {
                        if (!KeepAlive)
                        {
                            redis?.Close();
                            redis?.Dispose();
                            redis = null;
                        }
                        return new cResult(false, msg[0].Remove(0, 1));
                    }
                    // тип Массив (ПРИМЕРНОЕ РЕШЕНИЕ!!!)
                    else if (msg[0][0] == '*')
                    {
                        int arrStartAt = 2 + msg[0].IndexOf("\r\n");
                        int arrLen = Convert.ToInt32(msg[0].Substring(1, arrStartAt));
                        List<string> newArr = new List<string>();
                        for (int j = 1; j < 2*arrLen; j+=2)
                            newArr.Add(msg[1 + j]);
                        if (!KeepAlive)
                        {
                            redis?.Close();
                            redis?.Dispose();
                            redis = null;
                        }
                        return new cResult(true, newArr.ToArray());
                    }
                    // неизвестный ответ
                    else
                    {
                        if (!KeepAlive)
                        {
                            redis?.Close();
                            redis?.Dispose();
                            redis = null;
                        }
                        return new cResult(false, msg[0]);
                    }
                }
            }
            catch (Exception e)
            {
                // произошла внутренняя ошибка в коде выполнения программы
                if (!KeepAlive)
                {
                    redis?.Close();
                    redis?.Dispose();
                    redis = null;
                }
                return new cResult(false, e.Message);
            }

            // что то пошло не так
            if (!KeepAlive)
            {
                redis?.Close();
                redis?.Dispose();
                redis = null;
            }
            return new cResult(false, "Internal error of parsing length of returned value.");
        }

        /// <summary>
        /// Проверяет текст. Если в тексте содержатся пробелы, то заключает текст в кавычки.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string ValidateValue(string value)
        {
            string ret = "";
            if (value.IndexOf(" ") >= 0)
                ret = "\"" + value + "\"";
            else
                ret = value;

            return ret;
        }

        /// <summary>
        /// Кодирует команду в формат RESP
        /// </summary>
        /// <param name="cmd">Запрос к Redis в виде строки</param>
        /// <returns>Строка в формате RESP</returns>
        string ConvertToRESP(string cmd)
        {
            string res = "";
            long count = 0;

            foreach (Match m in Regex.Matches(cmd, @"(?<=\s|^)""((?:.(?!\s""\S))+?)""(?:\s|$)|\S+"))
            {
                if (m.Groups.Count == 2)
                {
                    if (m.Groups[1].Length != 0)
                    {
                        res += "$" + m.Groups[1].Value.Length.ToString() + "\r\n";
                        res += m.Groups[1].Value + "\r\n";
                    }
                    else
                    {
                        res += "$" + m.Value.Length.ToString() + "\r\n";
                        res += m.Value + "\r\n";
                    }
                    count++;
                }
            }

            res = "*" + count.ToString() + "\r\n" + res;

            return res;
        }


        // -- некоторые стандартные функции Redis -------------------------------------------------
        // примечание: для очереди FIFO нужно вызвать: lpush, rpop
        public class cSomeStandardFunctions
        {
            private cRedis parent;
            public cSomeStandardFunctions(cRedis parent)
            {
                this.parent = parent;
            }

            public cResult SetKey(string key, object value)
            {
                if (this.parent == null) return new cResult(false, null);

                return this.parent.SendCommand($"SET \"{key}\" \"{value.ToString()}\"");
            }

            public string GetKey(string key)
            {
                if (this.parent == null) return string.Empty;

                cResult r = this.parent.SendCommand($"GET \"{key}\"");
                if (!r.status) return null;
                if (r.result == null) return null;
                return r.result.ToString();
            }

            public bool KeyExists(string key)
            {
                if (this.parent == null) return false;

                int ret = 0;
                cResult r = this.parent.SendCommand($"EXISTS \"{key}\"");
                if (!r.status) return false;
                int.TryParse(r.result.ToString(), out ret);
                return (ret > 0);
            }

            public bool DelKey(string key)
            {
                if (this.parent == null) return false;

                int ret = 0;
                cResult r = this.parent.SendCommand($"DEL \"{key}\"");
                if (!r.status) return false;
                int.TryParse(r.result.ToString(), out ret);
                return (ret > 0);
            }

            public int QueueLen(string key)
            {
                if (this.parent == null) return 0;

                int ret = 0;
                cResult r = this.parent.SendCommand($"LLEN \"{key}\"");
                if (!r.status) return 0;
                int.TryParse(r.result.ToString(), out ret);
                return ret;
            }

            public string QueueRPop(string key)
            {
                if (this.parent == null) return string.Empty;

                cResult r = this.parent.SendCommand($"RPOP \"{key}\"");
                if (!r.status) return null;
                if (r.result == null) return null;
                return r.result.ToString();
            }

            public string QueueLPop(string key)
            {
                if (this.parent == null) return string.Empty;

                cResult r = this.parent.SendCommand($"LPOP \"{key}\"");
                if (!r.status) return null;
                if (r.result == null) return null;
                return r.result.ToString();
            }

            public int QueueRPush(string key, object value)
            {
                if (this.parent == null) return 0;

                int ret = 0;
                cResult r = this.parent.SendCommand($"RPUSH \"{key}\" \"{value.ToString()}\"");
                if (!r.status) return 0;
                int.TryParse(r.result.ToString(), out ret);
                return ret;
            }

            public int QueueLPush(string key, object value)
            {
                if (this.parent == null) return 0;

                int ret = 0;
                cResult r = this.parent.SendCommand($"LPUSH \"{key}\" \"{value.ToString()}\"");
                if (!r.status) return 0;
                int.TryParse(r.result.ToString(), out ret);
                return ret;
            }
        }
    }
}