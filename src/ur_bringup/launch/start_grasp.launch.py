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
            "target_x": LaunchConfiguration("target_x"),
            "target_y": LaunchConfiguration("target_y"),
            "target_z": LaunchConfiguration("target_z"),
            "pick_offset_x": LaunchConfiguration("pick_offset_x"),
            "pick_offset_y": LaunchConfiguration("pick_offset_y"),
            "pick_offset_z": LaunchConfiguration("pick_offset_z"),
        }.items(),
    )

    nodes_to_launch = [

        grasp_launch,
    ]

    return nodes_to_launch


def generate_launch_description():
    declared_arguments = [
        DeclareLaunchArgument("target_x", default_value="-0.15", description="Target place x in world frame."),
        DeclareLaunchArgument("target_y", default_value="-0.25", description="Target place y in world frame."),
        DeclareLaunchArgument("target_z", default_value="1.04", description="Target place z in world frame."),
        DeclareLaunchArgument("pick_offset_x", default_value="0.0", description="Pick offset x in world frame."),
        DeclareLaunchArgument("pick_offset_y", default_value="0.0", description="Pick offset y in world frame."),
        DeclareLaunchArgument("pick_offset_z", default_value="0.0", description="Pick offset z in world frame."),
    ]

    return LaunchDescription(declared_arguments + [OpaqueFunction(function=launch_setup)])
