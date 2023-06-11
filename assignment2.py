import grpc
from concurrent import futures
import computeandstorage_pb2
import computeandstorage_pb2_grpc
import boto3

class EC2OperationsServicer(computeandstorage_pb2_grpc.EC2OperationsServicer):

    s3 = boto3.client(
    's3',
    aws_access_key_id="ASIAYI2PO3XAQJQ2HBXL",
    aws_secret_access_key="8z3nzUie5b3RxEWGAksxOK7Cwb2TIMM+P7VIRlQI",
    aws_session_token="FwoGZXIvYXdzEIr//////////wEaDOpWXX2BehbYH+FTeiLAAXygIYMQGb5gmAL3Nd1mYgsraJAvmH+paGNkZ3SCP8w0dW7TdEzoDT/fiO1Z7dlZ1eBMLEzBlNw2XjgldyLPaH5gQY2xU1Fzi3WVjFrNQBb6cHyMKgW7nubxlBLv45Ulan13htjQfT8ZHOxU/CFnOTDGQckblZgtgOagBcxM/h/BalCaImUFkcr+O7XZhpOoCBVzD8QG86LzM0AXezYegRwhb9ckNHrk/iQOKiskCMMarUKk/0j+NcaDuvhRdApzUiik8JekBjItD2GlX/vHW6qvmQDRjLIU1/sO04IoFiqieQq0zZjdS49ZGex8+4Z0MvtnAfuJ"
    )
    def StoreData(self, request, context):
        data = request.data    
        self.s3.put_object(Body=data, Bucket='altamashawsbucket', Key='altamash.txt')
        url = f'http://s3.amazonaws.com/{bucket}/'
        response = computeandstorage_pb2.StoreReply(s3uri=url)
        return response

    def AppendData(self, request, context):
        data = request.data    
        cur = self.s3.get_object(Bucket='altamashawsbucket', Key='altamash.txt')['Body'].read().decode()
        new = cur + data
        self.s3.put_object(Body=new, Bucket='altamashawsbucket', Key='altamash.txt')
        response = computeandstorage_pb2.AppendReply()
        return response


    def DeleteFile(self, request, context):
        s3uri = request.s3uri
        self.s3.delete_object(Bucket='altamashawsbucket', Key='altamash.txt')
        response = computeandstorage_pb2.DeleteReply()
        return response


def run_server():
    server = grpc.server(futures.ThreadPoolExecutor())
    computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(EC2OperationsServicer(), server)
    server.add_insecure_port('[::]:5051')  
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    run_server()

