import os

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.conditions import IfCondition
from launch.substitutions import Command, EnvironmentVariable, FindExecutable, LaunchConfiguration, PathJoinSubstitution
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

    monitor_overrides = {
        "monitor_log_file": PathJoinSubstitution(
            [EnvironmentVariable("HOME"), "ros2_ws", "log", "fcl_monitor", "fcl_monitor.csv"]
        ),
    }

    execution_mode_overrides = {
        "enable_master_slave_sequencing": LaunchConfiguration("enable_master_slave_sequencing"),
        "enable_staggered_dual_arm_execution": LaunchConfiguration("enable_staggered_dual_arm_execution"),
        "primary_start_arm": LaunchConfiguration("primary_start_arm"),
        "secondary_start_delay": float(LaunchConfiguration("secondary_start_delay").perform(context)),
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
            monitor_overrides,
            execution_mode_overrides,
        ],
    )

    fcl_safety_monitor_node = Node(
        package="ur5e_gripper_control",
        executable="fcl_safety_monitor",
        name="fcl_safety_monitor",
        output="screen",
        condition=IfCondition(LaunchConfiguration("enable_fcl_monitor")),
        parameters=[
            {"use_sim_time": use_sim_time},
            robot_description,
            robot_description_semantic,
            robot_description_kinematics,
            robot_description_planning,
            parameter_file,
            monitor_overrides,
        ],
    )

    return [simple_pick_place_node, fcl_safety_monitor_node]


def generate_launch_description():
    declared_arguments = [
        DeclareLaunchArgument("enable_master_slave_sequencing", default_value="false"),
        DeclareLaunchArgument("enable_staggered_dual_arm_execution", default_value="true"),
        DeclareLaunchArgument("primary_start_arm", default_value="right"),
        DeclareLaunchArgument("secondary_start_delay", default_value="0.8"),
        DeclareLaunchArgument("enable_fcl_monitor", default_value="true"),
        DeclareLaunchArgument("description_package", default_value="ur5e_gripper_moveit_config"),
        DeclareLaunchArgument("description_file", default_value="ur5e_gripper.urdf.xacro"),
        DeclareLaunchArgument("moveit_config_package", default_value="ur5e_gripper_moveit_config"),
        DeclareLaunchArgument("moveit_config_file", default_value="ur5e_gripper.srdf.xacro"),
        DeclareLaunchArgument("moveit_joint_limits_file", default_value="joint_limits.yaml"),
        DeclareLaunchArgument("use_sim_time", default_value="true"),
    ]

    return LaunchDescription(declared_arguments + [OpaqueFunction(function=launch_setup)])
