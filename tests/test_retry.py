import unittest
import grpc
from uuid import uuid4

from logservice_protocol import log_pb2, log_pb2_grpc


class DeduplicationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to the running gRPC logging service
        cls.channel = grpc.insecure_channel("localhost:8001")
        cls.stub = log_pb2_grpc.LogServiceStub(cls.channel)

    def test_duplicate_uuid_detection(self):
        message_id = str(uuid4())
        msg = "Test message for deduplication"

        # First attempt should succeed
        first_response = self.stub.LogMessage(log_pb2.LogRequest(id=message_id, msg=msg))
        self.assertEqual(first_response.status, "Message logged")
        self.assertEqual(first_response.id, message_id)

        # Second attempt with same UUID should fail (deduplication)
        with self.assertRaises(grpc.RpcError) as context:
            self.stub.LogMessage(log_pb2.LogRequest(id=message_id, msg=msg))

        self.assertEqual(context.exception.code(), grpc.StatusCode.ALREADY_EXISTS)

    @classmethod
    def tearDownClass(cls):
        cls.channel.close()


if __name__ == "__main__":
    unittest.main()
