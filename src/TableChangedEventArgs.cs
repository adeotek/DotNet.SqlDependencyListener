using System;
using System.Text.Json;

namespace Adeotek.SqlDependencyListener;

public class TableChangedEventArgs<T> : EventArgs
{
    public string? Message { get; }
    public T? Data { get; }

    public TableChangedEventArgs(string notificationMessage)
    {
        Message = notificationMessage;

        if (notificationMessage is T message)
        {
            Data = message;
        }
        else
        {
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