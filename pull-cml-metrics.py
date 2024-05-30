import cmlapi
from cmlapi.rest import ApiException
import os

client = cmlapi.default_client(url=os.getenv("CDSW_API_URL").replace("/api/v1", ""), cml_api_key=os.getenv("CDSW_APIV2_KEY"))

# Function to get resource consumption for all jobs
def get_job_resources(project_id):
        project = client.get_project(project_id=project_id)
        jobs = client.list_jobs(project_id=project_id, page_size=1000).jobs
        
        resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        
        for job in jobs:
            try:
                resources["cpu"] += job.cpu
                resources["memory"] += job.memory
                resources["gpu"] += job.nvidia_gpu
            except AttributeError as e:
                print(f"Error processing job {job}: {e}")
        return resources

# Function to get resource consumption for all applications
def get_application_resources(project_id):
        project = client.get_project(project_id=project_id)
        applications = client.list_applications(project_id=project_id, page_size=1000).applications
        
        resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        
        for application in applications:
            try:
                resources["cpu"] += application.cpu
                resources["memory"] += application.memory
                resources["gpu"] += application.nvidia_gpu
            except AttributeError as e:
                print(f"Error processing job {job}: {e}")
        return resources
    
# Function to get resource consumption for all models
def get_model_resources(project_id):
        project = client.get_project(project_id=project_id)
        models = client.list_models(project_id=project_id, page_size=1000).models
        
        resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        
        for model in models:
            try:
                resources["cpu"] += (int(model.default_resources.cpu_millicores) / 1000)
                resources["memory"] += (int(model.default_resources.memory_mb) / 1024)
                resources["gpu"] += int(model.default_resources.nvidia_gpus)
            except AttributeError as e:
                print(f"Error processing job {job}: {e}")
        return resources
    
    
# Function to aggregate resource consumption across all projects
def aggregate_resources():
    try:
        all_projects = client.list_projects(page_size=1000).projects
        total_resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        total_job_resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        total_model_resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        total_app_resources = {
            "cpu": 0,
            "memory": 0,
            "gpu": 0
        }
        
        for project in all_projects:
            project_id = project.id
            
            job_resources = get_job_resources(project_id)
            if job_resources:
                total_job_resources["cpu"] += job_resources["cpu"]
                total_job_resources["memory"] += job_resources["memory"]
                total_job_resources["gpu"] += job_resources["gpu"]
            
            application_resources = get_application_resources(project_id)
            if application_resources:
                total_app_resources["cpu"] += application_resources["cpu"]
                total_app_resources["memory"] += application_resources["memory"]
                total_app_resources["gpu"] += application_resources["gpu"]
                
            model_resources = get_model_resources(project_id)
            if model_resources:
                total_model_resources["cpu"] += model_resources["cpu"]
                total_model_resources["memory"] += model_resources["memory"]
                total_model_resources["gpu"] += model_resources["gpu"]
            
        # Aggregate total resources from jobs and applications
        total_resources["cpu"] = total_job_resources["cpu"] + total_app_resources["cpu"]
        total_resources["memory"] = total_job_resources["memory"] + total_app_resources["memory"]
        total_resources["gpu"] = total_job_resources["gpu"] + total_app_resources["gpu"]

        return total_job_resources, total_app_resources, total_model_resources, total_resources
    
    except ApiException as e:
        print(f"Exception when calling ProjectsApi->list_projects: {e}")
        return None

# Main function
if __name__ == "__main__":
    total_job_resources, total_app_resources, total_model_resources, total_resources = aggregate_resources()

    print("Total Job Resources Consumption Across All Projects:")
    print(f"CPU: {total_job_resources['cpu']} cores")
    print(f"Memory: {total_job_resources['memory']} GB")
    print(f"GPU: {total_job_resources['gpu']} units\n")

    print("Total Application Resources Consumption Across All Projects:")
    print(f"CPU: {total_app_resources['cpu']} cores")
    print(f"Memory: {total_app_resources['memory']} GB")
    print(f"GPU: {total_app_resources['gpu']} units\n") 

    print("Total Model Resources Consumption Across All Projects:")
    print(f"CPU: {total_model_resources['cpu']} cores")
    print(f"Memory: {total_model_resources['memory']} GB")
    print(f"GPU: {total_model_resources['gpu']} units\n") 
    
    print("Total Resource Consumption Across All Projects:")
    print(f"CPU: {total_resources['cpu']} cores")
    print(f"Memory: {total_resources['memory']} GB")
    print(f"GPU: {total_resources['gpu']} units\n")
