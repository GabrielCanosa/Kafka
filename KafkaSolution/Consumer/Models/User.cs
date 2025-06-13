using Avro;
using Avro.Specific;

namespace Consumer.Models
{
    public class User : ISpecificRecord
    {
        public static Schema _SCHEMA = Schema.Parse(
            @"{
              ""type"":""record"",
              ""name"":""User"",
              ""namespace"":""Producer.Models"",
              ""fields"":[
                  {""name"":""name"",""type"":""string""},
                  {""name"":""age"",""type"":""int""}
              ]
            }");

        public virtual Schema Schema => _SCHEMA;

        public string Name { get; set; }
        public int Age { get; set; }

        public object Get(int fieldPos)
        {
            return fieldPos switch
            {
                0 => Name,
                1 => Age,
                _ => throw new AvroRuntimeException("Bad index " + fieldPos)
            };
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: Name = (string)fieldValue; break;
                case 1: Age = (int)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos);
            }
        }
    }
}
