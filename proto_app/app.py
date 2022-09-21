import aws_cdk as cdk

from python.stack import ProtoAppStack


app = cdk.App()
ProtoAppStack(app, "ProtoAppStack")
app.synth()
