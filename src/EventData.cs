using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Adeotek.SqlDependencyListener
{
    public class EventData<T>
    {
        [JsonPropertyName("event_type")]
        public string EventType { get; set; }
        [JsonPropertyName("inserted")]
        public List<T> Inserted { get; set; }
        [JsonPropertyName("deleted")]
        public List<T> Deleted { get; set; }
    }
}