from metaflow import FlowSpec, step, batch, environment, resources
# token: basicbatchflow-0-drkv
class BatchFlow(FlowSpec):

    @step
    def start(self):
        print("hello from metaflow")
        
        # 👇 This becomes an artifact
        self.message = "hello artifact world"

        self.next(self.step_0)

    @step
    def step_0(self):
        print("Step 0.")
        self.next(self.step_1,self.step_2,self.step_3,self.step_4)

    @batch()
    @step
    def step_1(self):
        print("Running on AWS batch using the default queue.")
        self.next(self.join)

    @batch(queue="metaflow-aws-fargate-queue")
    @step
    def step_2(self):
        print("Running on AWS batch using the fargate queue.")
        self.next(self.join)

    @batch(queue="metaflow-aws-ec2-queue")
    @step
    def step_3(self):
        print("Running on AWS batch using the ec2 queue.")
        self.next(self.join)

    @resources(gpu=1)
    @batch(queue="metaflow-aws-ec2-queue",image="pytorch/pytorch:2.13.0-cuda12.6-cudnn9-runtime")
    @environment(
        vars={
            "PIP_BREAK_SYSTEM_PACKAGES":"1" # pytorch image doesnt allow standard pip installations
        }
    )
    @step
    def step_4(self):
        print("Running on AWS batch using the ec2 queue and GPUs.")
        # Check CUDA availability
        import torch
        cuda_available = torch.cuda.is_available()
        print(f"CUDA available: {cuda_available}")

        if not cuda_available:
            raise RuntimeError("CUDA is not available")

        # Number of GPUs visible to this container
        gpu_count = torch.cuda.device_count()
        print(f"GPU count: {gpu_count}")

        # Information about each visible GPU
        for i in range(gpu_count):
            print(f"GPU {i}: {torch.cuda.get_device_name(i)}")

            props = torch.cuda.get_device_properties(i)
            print(f"  Total memory: {props.total_memory / 1024**3:.2f} GB")
            print(f"  CUDA capability: {props.major}.{props.minor}")

            # Use the first GPU
            device = torch.device("cuda:0")

            # Simple GPU computation
            self.x = torch.tensor([1.0, 2.0, 3.0], device=device)
            self.y = torch.tensor([4.0, 5.0, 6.0], device=device)

            self.result = self.x + self.y

            # Synchronize so any CUDA errors surface here
            torch.cuda.synchronize()

            print(f"x device: {self.x.device}")
            print(f"y device: {self.y.device}")
            print(f"result: {self.result}")
            print(f"result device: {self.result.device}")

        self.next(self.join)

    @step
    def join(self,inputs):
        print("All branches completed")
        self.next(self.end)
        
    @step
    def end(self):
        print("done")

if __name__ == "__main__":
    BatchFlow()