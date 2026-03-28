from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription, OpaqueFunction
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration
from launch_ros.substitutions import FindPackageShare


def launch_setup(context, *args, **kwargs):


    grasp_launch = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            [FindPackageShare("ur5e_gripper_control"), "/launch", "/simple_pick_place.launch.py"]
        ),
        launch_arguments={
            "enable_master_slave_sequencing": LaunchConfiguration("enable_master_slave_sequencing"),
            "enable_staggered_dual_arm_execution": LaunchConfiguration("enable_staggered_dual_arm_execution"),
            "primary_start_arm": LaunchConfiguration("primary_start_arm"),
            "secondary_start_delay": LaunchConfiguration("secondary_start_delay"),
        }.items(),
    )

    nodes_to_launch = [

        grasp_launch,
    ]

    return nodes_to_launch


def generate_launch_description():
    declared_arguments = [
        DeclareLaunchArgument("enable_master_slave_sequencing", default_value="false"),
        DeclareLaunchArgument("enable_staggered_dual_arm_execution", default_value="true"),
        DeclareLaunchArgument("primary_start_arm", default_value="right"),
        DeclareLaunchArgument("secondary_start_delay", default_value="0.8"),
    ]

    return LaunchDescription(declared_arguments + [OpaqueFunction(function=launch_setup)])
