using System;
using Abc.Zebus.Serialization.Protobuf;

namespace Abc.Zebus.Transport
{
    // TODO CAO: check null strings

    public static class TransportMessageWriter
    {
        internal static void Write(CodedOutputStream output, TransportMessage transportMessage)
        {
            output.WriteRawTag(10);
            Write(output, transportMessage.Id);

            output.WriteRawTag(18);
            Write(output, transportMessage.MessageTypeId);

            var length = transportMessage.Content.Length;
            if (length != 0)
            {
                output.WriteRawTag(26);
                output.WriteLength((int)length);
                output.WriteRawBytes(transportMessage.Content);
            }

            output.WriteRawTag(34);
            Write(output, transportMessage.Originator);

            if (transportMessage.Environment != null)
            {
                output.WriteRawTag(42);
                output.WriteString(transportMessage.Environment);
            }

            if (transportMessage.WasPersisted != null)
            {
                output.WriteRawTag(48);
                output.WriteBool(transportMessage.WasPersisted.Value);
            }
        }

        private static void Write(CodedOutputStream output, MessageId messageId)
        {
            var size = 1 + GetMessageSizeWithLength(CalculateSize(messageId.Value));
            output.WriteLength(size);
            output.WriteRawTag(10);
            Write(output, messageId.Value);
        }

        private static void Write(CodedOutputStream output, Guid guid)
        {
            output.WriteLength(CalculateSize(guid));

            var blob = guid.ToByteArray();
            output.WriteRawTag(9);
            output.WriteRawBytes(blob, 0, 8);
            output.WriteRawTag(17);
            output.WriteRawBytes(blob, 8, 8);
        }

        private static int CalculateSize(Guid guid)
        {
            return 1 + 8 + 1 + 8;
        }

        private static void Write(CodedOutputStream output, MessageTypeId messageTypeId)
        {
            var size = 1 + CodedOutputStream.ComputeStringSize(messageTypeId.FullName);
            output.WriteLength(size);
            output.WriteRawTag(10);
            output.WriteString(messageTypeId.FullName);
        }

        private static void Write(CodedOutputStream output, OriginatorInfo originatorInfo)
        {
            var size = ComputeSize(originatorInfo);
            output.WriteLength(size);
            output.WriteRawTag(10);
            Write(output, originatorInfo.SenderId);
            output.WriteRawTag(18);
            output.WriteString(originatorInfo.SenderEndPoint);
            output.WriteRawTag(26);
            output.WriteString(originatorInfo.SenderMachineName);
            output.WriteRawTag(42);
            output.WriteString(originatorInfo.InitiatorUserName);
        }

        private static int ComputeSize(OriginatorInfo originatorInfo)
        {
            var size = 0;
            size += 1 + GetMessageSizeWithLength(1 + CodedOutputStream.ComputeStringSize(originatorInfo.SenderId.ToString()));
            size += 1 + CodedOutputStream.ComputeStringSize(originatorInfo.SenderEndPoint);
            size += 1 + CodedOutputStream.ComputeStringSize(originatorInfo.SenderMachineName);
            size += 1 + CodedOutputStream.ComputeStringSize(originatorInfo.InitiatorUserName);
            return size;
        }

        private static void Write(CodedOutputStream output, PeerId peerId)
        {
            var size = 1 + CodedOutputStream.ComputeStringSize(peerId.ToString());
            output.WriteLength(size);
            output.WriteRawTag(10);
            output.WriteString(peerId.ToString());
        }

        private static int GetMessageSizeWithLength(int size)
        {
            return size + CodedOutputStream.ComputeLengthSize(size);
        }
    }
}