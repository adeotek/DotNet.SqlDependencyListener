using System;
using System.Text.Json;

namespace Adeotek.SqlDependencyListener
{
    public class TableChangedEventArgs<T> : EventArgs
    {
        public string Message { get; }
        public T Data { get; }

        public TableChangedEventArgs(string notificationMessage)
        {
            Message = notificationMessage;

            if (Message is T message)
            {
                Data = message;
            }

            try
            {
                Data = string.IsNullOrEmpty(Message) ? default : JsonSerializer.Deserialize<T>(Message);
            }
            catch (Exception)
            {
                Data = default;
            }
        }
    }
}