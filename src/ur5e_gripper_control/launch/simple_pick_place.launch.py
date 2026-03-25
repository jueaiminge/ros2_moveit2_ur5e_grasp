import os

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import Command, FindExecutable, LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare
from ur_moveit_config.launch_common import load_yaml


def launch_setup(context, *args, **kwargs):
    description_package = LaunchConfiguration("description_package")
    description_file = LaunchConfiguration("description_file")
    moveit_config_package = LaunchConfiguration("moveit_config_package")
    moveit_joint_limits_file = LaunchConfiguration("moveit_joint_limits_file")
    moveit_config_file = LaunchConfiguration("moveit_config_file")
    use_sim_time = LaunchConfiguration("use_sim_time")

    robot_description_content = Command(
        [
            PathJoinSubstitution([FindExecutable(name="xacro")]),
            " ",
            PathJoinSubstitution([FindPackageShare(description_package), "urdf", description_file]),
        ]
    )
    robot_description = {"robot_description": robot_description_content}

    robot_description_semantic_content = Command(
        [
            PathJoinSubstitution([FindExecutable(name="xacro")]),
            " ",
            PathJoinSubstitution(
                [FindPackageShare(moveit_config_package), "srdf", moveit_config_file]
            ),
        ]
    )
    robot_description_semantic = {"robot_description_semantic": robot_description_semantic_content}

    robot_description_kinematics = PathJoinSubstitution(
        [FindPackageShare(moveit_config_package), "config", "kinematics.yaml"]
    )

    robot_description_planning = {
        "robot_description_planning": load_yaml(
            str(moveit_config_package.perform(context)),
            os.path.join("config", str(moveit_joint_limits_file.perform(context))),
        )
    }

    parameter_file = PathJoinSubstitution(
        [FindPackageShare("ur5e_gripper_control"), "config", "simple_pick_place.yaml"]
    )

    target_override = {
        "target_position": [
            float(LaunchConfiguration("target_x").perform(context)),
            float(LaunchConfiguration("target_y").perform(context)),
            float(LaunchConfiguration("target_z").perform(context)),
        ]
    }

    source_offset_override = {
        "source_position_offset": [
            float(LaunchConfiguration("pick_offset_x").perform(context)),
            float(LaunchConfiguration("pick_offset_y").perform(context)),
            float(LaunchConfiguration("pick_offset_z").perform(context)),
        ]
    }

    simple_pick_place_node = Node(
        package="ur5e_gripper_control",
        executable="simple_pick_place",
        name="simple_pick_place",
        output="screen",
        parameters=[
            {"use_sim_time": use_sim_time},
            robot_description,
            robot_description_semantic,
            robot_description_kinematics,
            robot_description_planning,
            parameter_file,
            target_override,
            source_offset_override,
        ],
    )

    return [simple_pick_place_node]


def generate_launch_description():
    declared_arguments = [
        DeclareLaunchArgument("target_x", default_value="-0.15", description="Target place x in world frame."),
        DeclareLaunchArgument("target_y", default_value="-0.25", description="Target place y in world frame."),
        DeclareLaunchArgument("target_z", default_value="1.04", description="Target place z in world frame."),
        DeclareLaunchArgument("pick_offset_x", default_value="-0.012", description="Pick offset x in world frame."),
        DeclareLaunchArgument("pick_offset_y", default_value="0.004", description="Pick offset y in world frame."),
        DeclareLaunchArgument("pick_offset_z", default_value="0.0", description="Pick offset z in world frame."),
        DeclareLaunchArgument("description_package", default_value="ur5e_gripper_moveit_config"),
        DeclareLaunchArgument("description_file", default_value="ur5e_gripper.urdf.xacro"),
        DeclareLaunchArgument("moveit_config_package", default_value="ur5e_gripper_moveit_config"),
        DeclareLaunchArgument("moveit_config_file", default_value="ur5e_gripper.srdf.xacro"),
        DeclareLaunchArgument("moveit_joint_limits_file", default_value="joint_limits.yaml"),
        DeclareLaunchArgument("use_sim_time", default_value="true"),
    ]

    return LaunchDescription(declared_arguments + [OpaqueFunction(function=launch_setup)])
