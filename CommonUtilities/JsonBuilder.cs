using System.Text;

namespace CommonUtilities
{
    public static class JsonBuilder
    {
        public static StringBuilder AppendJson(this StringBuilder sb, string serializedJson)
        {
            return sb.Append(serializedJson).Append(',');
        }

        public static StringBuilder StartJsonArray(this StringBuilder sb)
        {
            return sb.Append('[');
        }

        public static StringBuilder EndJsonArray(this StringBuilder sb)
        {
            return sb.Append(']');
        }

        public static string ToJsonValue(this object obj)
        {
            if (obj is null)
            {
                return "null";
            }
            else if (obj is bool)
            {
                return ((bool)obj).ToString().ToLowerInvariant();
            }
            else
            {
                return $"{obj}";
            }
        }
    }
}